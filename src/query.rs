// Use BufMut to store the written bytes. Write into it using encode_length_delimited. Then write files as
/*File::create("filename").write_all(&b[..])?*/

fn generate_heartbeat_response(
    latest_sent: Option<LogEntry>,
    latest_match: Option<LogEntry>,
    old_received: usize,
    current_term: usize,
    message_last_entry: Option<LogEntry>,
) -> RaftManagementResponse {
    let mut new_received = old_received;
    if let Some(last) = message_last_entry {
        new_received = last.get_index();
    }

    let mut response = RaftManagementResponse::HeartbeatOk {
        max_received: new_received,
        current_term: current_term,
    };

    match (latest_sent, latest_match) {
        (Some(x), Some(y)) => {
            if x != y {
                // latest_sent did not equal the same spot in current log, need to add-one
                response = RaftManagementResponse::HeartbeatAddOne {
                    max_received: old_received.min(x.get_index()),
                };
                eprintln!("Responding with {:?}", response);
            }
        }
        (Some(x), None) => {
            // The entry did not have a corresponding match, need to call for a heartbeat add-one
            response = RaftManagementResponse::HeartbeatAddOne {
                max_received: old_received.min(x.get_index()),
            };
        }
        (_, _) => {}
    }

    return response;
}
