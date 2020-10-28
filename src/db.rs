use tokio::sync::{Notify, broadcast};
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use tokio::time::{Instant, Duration};
use std::collections::{HashMap, BTreeMap};

#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Shared>
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
    // 发布订阅模式
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    // 将有过期时间的key放到btree结构中
    expirations: BTreeMap<(Instant, u64), String>,
    next_id: u64,
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    id: u64,
    data: Bytes,
    expires_at: Option<Instant>,
}


impl Db {
    // 构造函数
    pub(crate) fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 开启另外一个协程处理background 任务
        tokio::task::spawn(purge_expired_tasks(shared.clone()));

        Self {
            shared
        }
    }

    // get方法
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| {
            entry.data.clone()
        })
    }

    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap(); // 获取锁
        let id = state.next_id;
        state.next_id += 1;

        let mut notify = false; // 需不需要触发gc

        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration; // 啥时候到期

            notify = state.next_expiration().map(|expiration| {
                expiration > when
            }).unwrap_or(true);

            state.expirations.insert((when, id), key.clone());
            when
        });


        let prev = state.entries.insert(key, Entry {
            id,
            data: value,
            expires_at,
        });

        // {{  这尼玛的是map里面之前有值，插入了一个相同的key就是返回之前的值，所以才有这一步
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, prev.id));
            }
        }
        //}}

        // 后面需要全局notify 需要提前释放锁
        drop(state);

        if notify {
            self.shared.background_task.notify_one();
        }
    }
}

impl State {
    // 查找小的key
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations.keys().next().map(|instance| {
            instance.0
        })
    }
}

impl Shared {
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap(); // 先拿到state
        if state.shutdown { // 链接关闭了直接返回了
            return None;
        }

        // 取得state的可变引用
        let state = &mut *state;
        let now = Instant::now();
        while let Some((&(when, next_id), key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }
            // 过期了直接删除
            state.entries.remove(key);
            state.expirations.remove(&(when, next_id));
        }

        None
    }

    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() { // 没有结束一直在后台运行
        if let Some(when) = shared.purge_expired_keys() {
            tokio::select! {
                _ = tokio::time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 惰性删除，有新增就不会阻塞在这了
            shared.background_task.notified().await
        }
    }
}


