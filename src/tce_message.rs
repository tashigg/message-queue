use der::Encode;

#[derive(der::Sequence)]
pub struct Transaction {
    data: TransactionData,
}

#[derive(der::Choice)]
#[asn1(tag_mode = "EXPLICIT")]
pub enum TransactionData {
    #[asn1(context_specific = "2", constructed = "true")]
    Publish(PublishTrasaction),
}

#[derive(der::Sequence)]
pub struct PublishTrasaction {
    topic: String,
    payload: Vec<u8>,
}

impl TryFrom<Transaction> for tashi_consensus_engine::ApplicationTransaction {
    type Error = color_eyre::eyre::Error;

    fn try_from(value: Transaction) -> Result<Self, Self::Error> {
        value.to_der()?.try_into()
    }
}
