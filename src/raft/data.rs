use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

use crate::Object;

#[derive(Debug, Clone, PartialEq)]
pub enum LogEntry {
    Insert {
        key: String,
        data: Object,
        index: usize,
        term: usize,
    },
    Delete {
        key: String,
        index: usize,
        term: usize,
    },
    Cas {
        key: String,
        old_value: Object,
        new_value: Object,
        index: usize,
        term: usize,
    },
}

impl LogEntry {
    pub fn get_index(&self) -> usize {
        match self {
            LogEntry::Insert {
                key: _,
                data: _,
                index,
                term: _,
            } => *index,
            LogEntry::Delete {
                key: _,
                index,
                term: _,
            } => *index,
            LogEntry::Cas {
                key: _,
                old_value: _,
                new_value: _,
                index,
                term: _,
            } => *index,
        }
    }
    pub fn get_term(&self) -> usize {
        match self {
            LogEntry::Insert {
                key: _,
                data: _,
                index: _,
                term,
            } => *term,
            LogEntry::Delete {
                key: _,
                index: _,
                term,
            } => *term,
            LogEntry::Cas {
                key,
                old_value,
                new_value,
                index,
                term,
            } => *term,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RaftState {
    Leader(usize),
    Canidate(usize),
    Follower(usize, Option<SocketAddr>, bool),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftCore {
    pub max_committed: usize,
    pub max_received: usize,
    pub current_term: usize,
    pub members: Vec<SocketAddr>,
    pub address: SocketAddr,
    pub last_voted: usize,
}
