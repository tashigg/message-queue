#[path = "../../rumqtt/rumqttd/src/protocol/mod.rs"]
#[allow(warnings)]
mod protocol;

#[path = "../../rumqtt/rumqttd/src/router/mod.rs"]
#[allow(warnings)]
pub mod router;

#[path = "../../rumqtt/rumqttd/src/segments/mod.rs"]
#[allow(warnings)]
mod segments;

pub use router::{shared_subs::Strategy, Router, RouterConfig};

use router::{ConnectionId, Filter, Notification, RouterId, Topic};
use segments::Storage;

type Cursor = (u64, u64);
type Offset = (u64, u64);
