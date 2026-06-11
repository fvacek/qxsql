use shvclient::ClientCommandSender;
use shvproto::to_rpcvalue;
use async_trait::async_trait;

use crate::{RecChng, RecOp, Record};

#[async_trait]
pub trait QxSqlApiRecChng: super::QxSqlApi {
    fn filter_recchng(&self, recchng: RecChng) -> Option<RecChng>;
    fn client_command_sender(&self) -> Option<ClientCommandSender>;

    async fn create_record_with_recchng(&self, table: &str, record: &Record, issuer: Option<String>) -> anyhow::Result<i64> {
        let insert_id = self.create_record(table, record).await?;
        let recchng = RecChng {table: table.to_string(), id:insert_id, record:Some(record.clone()), op: RecOp::Insert, issuer };
        if let Some(recchng) = self.filter_recchng(recchng) && let Some(client_cmd_tx) = self.client_command_sender() {
            let rec = to_rpcvalue(&recchng)?;
            client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng").with_param(rec))?;
        }
        Ok(insert_id)
    }
    async fn update_record_with_recchng(&self, table: &str, id: i64, record: &Record, issuer: Option<String>) -> anyhow::Result<bool> {
        let updated = self.update_record(table, id, record).await?;
        if updated {
            let recchng = RecChng {table: table.to_string(), id, record:Some(record.clone()), op: RecOp::Update, issuer };
            if let Some(recchng) = self.filter_recchng(recchng) && let Some(client_cmd_tx) = self.client_command_sender() {
                let rec = to_rpcvalue(&recchng)?;
                client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng").with_param(rec))?;
            }
        }
        Ok(updated)
    }
    async fn delete_record_with_recchng(&self, table: &str, id: i64, issuer: Option<String>) -> anyhow::Result<bool> {
        let deleted = self.delete_record(table, id).await?;
        if deleted {
            let recchng = RecChng {table: table.to_string(), id, record:None, op: RecOp::Delete, issuer };
            if let Some(recchng) = self.filter_recchng(recchng) && let Some(client_cmd_tx) = self.client_command_sender() {
                let rec = to_rpcvalue(&recchng)?;
                client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng").with_param(rec))?;
            }
        }
        Ok(deleted)
    }
}
