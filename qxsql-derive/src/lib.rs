use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derives `ToRecord` for a struct, converting each named field into a
/// `qxsql::Record` (`HashMap<String, DbValue>`).
///
/// # Field attributes
/// - `#[to_record(skip)]`  — omit this field from the record
/// - `#[to_record(rename = "col_name")]` — use a different key in the record
///
/// Fields whose type is `Option<T>` are inserted as `DbValue::Null` when `None`.
/// If you want to omit `None` fields entirely, add `#[to_record(skip_if_none)]`.
#[proc_macro_derive(ToRecord, attributes(to_record))]
pub fn derive_to_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    impl_to_record(input)
        .unwrap_or_else(|e| e.to_compile_error().into())
}

fn impl_to_record(input: DeriveInput) -> Result<TokenStream, syn::Error> {
    let name = &input.ident;

    let Data::Struct(data_struct) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "ToRecord can only be derived for structs",
        ));
    };

    let Fields::Named(fields) = &data_struct.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "ToRecord requires a struct with named fields",
        ));
    };

    let mut inserts = Vec::new();

    for field in &fields.named {
        let field_ident = field.ident.as_ref().unwrap();

        // --- parse #[to_record(...)] attributes ---
        let mut skip = false;
        let mut rename: Option<String> = None;
        let mut skip_if_none = false;

        for attr in &field.attrs {
            if !attr.path().is_ident("to_record") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                    return Ok(());
                }
                if meta.path.is_ident("skip_if_none") {
                    skip_if_none = true;
                    return Ok(());
                }
                if meta.path.is_ident("rename") {
                    let value = meta.value()?;
                    let s: syn::LitStr = value.parse()?;
                    rename = Some(s.value());
                    return Ok(());
                }
                Err(meta.error("unknown to_record attribute"))
            })?;
        }

        if skip {
            continue;
        }

        let key = rename.unwrap_or_else(|| field_ident.to_string());

        // Detect Option<T> by checking the outermost type path segment.
        let is_option = is_option_type(&field.ty);

        let insert = if is_option {
            if skip_if_none {
                quote! {
                    if let Some(v) = &self.#field_ident {
                        record.insert(#key.to_string(), ::qxsql::DbValue::from(v.clone()));
                    }
                }
            } else {
                quote! {
                    match &self.#field_ident {
                        Some(v) => { record.insert(#key.to_string(), ::qxsql::DbValue::from(v.clone())); }
                        None    => { record.insert(#key.to_string(), ::qxsql::DbValue::Null); }
                    }
                }
            }
        } else {
            quote! {
                record.insert(#key.to_string(), ::qxsql::DbValue::from(self.#field_ident.clone()));
            }
        };

        inserts.push(insert);
    }

    let expanded = quote! {
        impl #name {
            pub fn to_record(&self) -> ::qxsql::Record {
                let mut record = ::qxsql::Record::new();
                #(#inserts)*
                record
            }
        }
    };

    Ok(expanded.into())
}

/// Derives `TryFromRecord` for a struct, building an instance from a
/// `qxsql::Record` (`HashMap<String, DbValue>`).
///
/// Each named field must implement `qxsql::FromDbValue` (all primitive DB
/// types and `Option<T>` do so automatically).
///
/// # Field attributes  (same namespace as `ToRecord`)
/// - `#[to_record(skip)]` — field is not read from the record; the field type
///   must implement `Default`.
/// - `#[to_record(rename = "col_name")]` — look up this key instead of the
///   field name.
/// - `#[to_record(skip_if_none)]` — accepted for parity with `ToRecord`;
///   has no extra effect during deserialization.
///
/// The generated method is:
/// ```ignore
/// fn try_from_record(record: qxsql::Record) -> Result<Self, String>
/// ```
#[proc_macro_derive(TryFromRecord, attributes(to_record))]
pub fn derive_try_from_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    impl_try_from_record(input)
        .unwrap_or_else(|e| e.to_compile_error().into())
}

fn impl_try_from_record(input: DeriveInput) -> Result<TokenStream, syn::Error> {
    let name = &input.ident;

    let Data::Struct(data_struct) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "TryFromRecord can only be derived for structs",
        ));
    };

    let Fields::Named(fields) = &data_struct.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "TryFromRecord requires a struct with named fields",
        ));
    };

    let mut field_inits = Vec::new();

    for field in &fields.named {
        let field_ident = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;

        // --- parse #[to_record(...)] attributes ---
        let mut skip = false;
        let mut rename: Option<String> = None;

        for attr in &field.attrs {
            if !attr.path().is_ident("to_record") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                    return Ok(());
                }
                if meta.path.is_ident("skip_if_none") {
                    // accepted for parity, no special handling needed
                    return Ok(());
                }
                if meta.path.is_ident("rename") {
                    let value = meta.value()?;
                    let s: syn::LitStr = value.parse()?;
                    rename = Some(s.value());
                    return Ok(());
                }
                Err(meta.error("unknown to_record attribute"))
            })?;
        }

        let init = if skip {
            quote! {
                #field_ident: <#field_ty as ::std::default::Default>::default(),
            }
        } else {
            let key = rename.unwrap_or_else(|| field_ident.to_string());
            let is_option = is_option_type(field_ty);

            if is_option {
                // Missing key and explicit NULL both map to None.
                quote! {
                    #field_ident: match record.remove(#key) {
                        Some(v) => <#field_ty as ::qxsql::FromDbValue>::from_db_value(v)
                            .map_err(|e| format!(concat!("field '", #key, "': {}"), e))?,
                        None => None,
                    },
                }
            } else {
                quote! {
                    #field_ident: {
                        let v = record.remove(#key)
                            .ok_or_else(|| format!("missing field '{}'", #key))?;
                        <#field_ty as ::qxsql::FromDbValue>::from_db_value(v)
                            .map_err(|e| format!(concat!("field '", #key, "': {}"), e))?
                    },
                }
            }
        };

        field_inits.push(init);
    }

    let expanded = quote! {
        impl #name {
            pub fn try_from_record(mut record: ::qxsql::Record) -> ::std::result::Result<Self, ::std::string::String> {
                Ok(Self {
                    #(#field_inits)*
                })
            }
        }
    };

    Ok(expanded.into())
}

/// Returns `true` when the outermost type is `Option<…>`.
fn is_option_type(ty: &syn::Type) -> bool {
    let syn::Type::Path(type_path) = ty else {
        return false;
    };
    let Some(last) = type_path.path.segments.last() else {
        return false;
    };
    last.ident == "Option"
}
