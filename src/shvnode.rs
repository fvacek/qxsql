use shvproto::{rpcvalue, RpcValue};
use shvrpc::metamethod::{self, AccessLevel, Flag, MetaMethod};

pub const METH_DIR: &str = "dir";
pub const METH_LS: &str = "ls";
pub const META_METHOD_PUBLIC_DIR: MetaMethod = MetaMethod { name: METH_DIR, flags: Flag::None as u32, access: AccessLevel::Browse, param: "DirParam", result: "DirResult", signals: &[], description: "" };
pub const META_METHOD_PUBLIC_LS: MetaMethod = MetaMethod { name: METH_LS, flags: Flag::None as u32, access: AccessLevel::Browse, param: "LsParam", result: "LsResult", signals: &[], description: "" };
pub const PUBLIC_DIR_LS_METHODS: [MetaMethod; 2] = [META_METHOD_PUBLIC_DIR, META_METHOD_PUBLIC_LS];

pub enum DirParam {
    Brief,
    Full,
    MethodExists(String),
}
impl From<Option<&RpcValue>> for DirParam {
    fn from(value: Option<&RpcValue>) -> Self {
        match value {
            Some(rpcval) => {
                if rpcval.is_string() {
                    DirParam::MethodExists(rpcval.as_str().into())
                } else if rpcval.as_bool() {
                    DirParam::Full
                } else {
                    DirParam::Brief
                }
            }
            None => {
                DirParam::Brief
            }
        }
    }
}

pub fn dir<'a>(mut methods: impl Iterator<Item=&'a MetaMethod>, param: DirParam) -> RpcValue {
    if let DirParam::MethodExists(ref method_name) = param {
        return methods.any(|mm| mm.name == method_name).into()
    }
    let mut lst = rpcvalue::List::new();
    for mm in methods {
        match param {
            DirParam::Brief => {
                lst.push(mm.to_rpcvalue(metamethod::DirFormat::IMap));
            }
            DirParam::Full => {
                lst.push(mm.to_rpcvalue(metamethod::DirFormat::Map));
            }
            _ => {}
        }
    }
    lst.into()
}

pub enum LsParam {
    List,
    Exists,
}
impl From<Option<&RpcValue>> for LsParam {
    fn from(value: Option<&RpcValue>) -> Self {
        match value {
            Some(rpcval) => {
                if rpcval.is_string() {
                    LsParam::Exists
                } else {
                    LsParam::List
                }
            }
            None => {
                LsParam::List
            }
        }
    }
}
