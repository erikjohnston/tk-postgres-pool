use postgres_protocol::Oid;
use postgres_protocol::message::backend::Message as BackendMessage;
use postgres_protocol::message::frontend::Message;

use std::io::Error as IoError;


pub fn backend_message_type(msg: &BackendMessage) -> &'static str {
    use BackendMessage::*;
    match *msg {
        AuthenticationOk => "AuthenticationOk",
        AuthenticationCleartextPassword => "AuthenticationCleartextPassword",
        AuthenticationGSS => "AuthenticationGSS",
        AuthenticationKerberosV5 => "AuthenticationKerberosV5",
        AuthenticationMD5Password { .. } => "AuthenticationMD5Password",
        AuthenticationSCMCredential => "AuthenticationSCMCredential",
        AuthenticationSSPI => "AuthenticationSSPI",
        BackendKeyData { .. } => "BackendKeyData",
        BindComplete => "BindComplete",
        CloseComplete => "CloseComplete",
        CommandComplete { .. } => "CommandComplete",
        CopyData { .. } => "CopyData",
        CopyDone => "CopyDone",
        CopyInResponse { .. } => "CopyInResponse",
        CopyOutResponse { .. } => "CopyOutResponse",
        DataRow { .. } => "DataRow",
        EmptyQueryResponse => "EmptyQueryResponse",
        ErrorResponse { .. } => "ErrorResponse",
        NoData => "NoData",
        NoticeResponse { .. } => "NoticeResponse",
        NotificationResponse { .. } => "NotificationResponse",
        ParameterDescription { .. } => "ParameterDescription",
        ParameterStatus { .. } => "ParameterStatus",
        ParseComplete => "ParseComplete",
        PortalSuspended => "PortalSuspended",
        ReadyForQuery { .. } => "ReadyForQuery",
        RowDescription { .. } => "RowDescription",
        _ => "Unknown",
    }
}


#[derive(Debug, Clone)]
pub enum FrontendMessage {
    Bind {
        portal: String,
        statement: String,
        formats: Vec<i16>,
        values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    CancelRequest { process_id: i32, secret_key: i32 },
    Close { variant: u8, name: String },
    Describe { variant: u8, name: String },
    Execute { portal: String, max_rows: i32 },
    Parse {
        name: String,
        query: String,
        param_types: Vec<Oid>,
    },
    PasswordMessage { password: String },
    Query { query: String },
    SslRequest,
    StartupMessage { parameters: Vec<(String, String)> },
    Sync,
    Terminate,
}

impl FrontendMessage {
    pub fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), IoError> {
        let message = match *self {
            FrontendMessage::Bind { ref portal,
                                    ref statement,
                                    ref formats,
                                    ref values,
                                    ref result_formats } => {
                Message::Bind {
                    portal: &portal,
                    statement: &statement,
                    formats: &formats,
                    values: &values,
                    result_formats: &result_formats,
                }
            }
            FrontendMessage::CancelRequest { process_id, secret_key } => {
                Message::CancelRequest {
                    process_id: process_id,
                    secret_key: secret_key,
                }
            }
            FrontendMessage::Close { variant, ref name } => {
                Message::Close {
                    variant: variant,
                    name: &name,
                }
            }
            FrontendMessage::Describe { variant, ref name } => {
                Message::Describe {
                    variant: variant,
                    name: &name,
                }
            }
            FrontendMessage::Execute { ref portal, max_rows } => {
                Message::Execute {
                    portal: &portal,
                    max_rows: max_rows,
                }
            }
            FrontendMessage::Parse { ref name, ref query, ref param_types } => {
                Message::Parse {
                    name: &name,
                    query: &query,
                    param_types: &param_types,
                }
            }
            FrontendMessage::PasswordMessage { ref password } => {
                Message::PasswordMessage { password: &password }
            }
            FrontendMessage::Query { ref query } => Message::Query { query: &query },
            FrontendMessage::SslRequest => Message::SslRequest,
            FrontendMessage::StartupMessage { ref parameters } => {
                Message::StartupMessage { parameters: &parameters }
            }
            FrontendMessage::Sync => Message::Sync,
            FrontendMessage::Terminate => Message::Terminate,
        };

        message.serialize(buf)
    }
}
