# Agent：

目前看了agent里的benches，config，crates，docker，examples，pkg，plugins，resources文件夹下的所有Rust代码（还有几个文件夹没看完），src中看了部分代码，其中优先看的k8s部分，上面那几个文件夹大多关于协议，操作系统，元数据包等有关，估计我们改不了（个人感觉）。k8s这部分因为有根据使用环境等自行改变的自定义资源类型等，估计在crd中有可添加的部分。

## deepflow中kubernetes部分：

### active_poller.cs:

```
/*这是一个基于 Rust 语言的 ActivePoller 实现，用于轮询命名空间和接口信息。

ActivePoller 是一个实现了 Poller trait 的结构体，用于轮询命名空间和接口信息。

interval: 轮询间隔。
version: AtomicU64 类型的原子变量，用于标识 entries 的更新。
entries: 一个包含命名空间和接口信息的 HashMap，存储了命名空间和对应的接口列表。
netns_regex: 命名空间的正则表达式，用于筛选需要轮询的命名空间。
running: 一个标识 Poller 是否正在运行的 Mutex<bool> 类型的变量。
timer: 一个用于定时触发轮询的 Condvar 类型的变量。
thread: Poller 的线程句柄。
ActivePoller 实现了 Poller trait 的方法，包括：

new: 创建一个 ActivePoller 实例。
start: 启动 Poller，开始轮询命名空间和接口信息。
stop: 停止 Poller 的轮询。
get_entries: 获取当前的命名空间和接口信息。

在实现中，ActivePoller 的 process 方法用于执行轮询逻辑。它会在启动时初始化命名空间和接口信息，然后进入一个循环，在循环中等待轮询间隔时间，更新命名空间和接口信息，并更新版本号。
如果 Poller 停止运行，循环退出。

start 方法会检查 Poller 是否已经在运行，如果是，则不执行重复启动。
否则，将标识 Poller 正在运行的 running 变量设为 true，创建一个线程来执行 process 方法，并将线程句柄保存在 thread 字段中。

stop 方法会检查 Poller 是否已经停止运行，如果是，则不执行重复停止。
否则，将标识 Poller 停止运行的 running 变量设为 false，通过调用 notify_one 方法通知等待的线程停止轮询，并等待线程结束。

get_entries 方法用于获取当前的命名空间和接口信息。它会获取 entries 字段的锁，并克隆出一个副本返回。

该实现在轮询过程中会查询指定的命名空间（包括根名称空间和符合正则表达式的额外命名空间），并将查询到的命名空间和对应的接口信息存储在 entries 字段中。
每次轮询开始时，会先更新命名空间和接口信息，并将版本号加一，以表示 entries 的更新。然后，根据新查询到的命名空间和接口信息更新已有的条目，如果某个命名空间不存在于新的查询结果中，
则增加其过期计数器，如果过期计数器达到最大值（ENTRY_EXPIRE_COUNT），则将该条目标记为过期，并从 entries 中移除。如果命名空间仍然存在于查询结果中，则重置其过期计数器。
最后，更新版本号，记录更新的次数。

整个轮询过程在一个无限循环中进行，直到 Poller 停止运行。当 Poller 停止运行时，会退出循环，并输出相应的日志。

该实现使用了多线程的方式执行轮询逻辑，通过线程间的锁和条件变量来实现线程同步。这样可以使得轮询过程在后台异步执行，不会阻塞主线程。

该ActivePoller用于轮询命名空间和接口信息，并根据查询结果进行更新和维护。它提供了启动、停止和获取当前命名空间和接口信息的接口，可以方便地与其他组件进行集成和使用。*/

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use log::{debug, info, log_enabled, trace, warn, Level};
use regex::Regex;

use super::Poller;
use public::netns::{self, InterfaceInfo, NsFile};

const ENTRY_EXPIRE_COUNT: u8 = 3; // 条目过期计数器的最大值

#[derive(Debug, Default)]
pub struct ActivePoller {
    interval: Duration, // 轮询间隔
    version: Arc<AtomicU64>, // 版本号，用于标识 entries 的更新
    entries: Arc<Mutex<HashMap<NsFile, Vec<InterfaceInfo>>>>, // 存储命名空间和接口信息的映射
    netns_regex: Arc<Mutex<Option<Regex>>>, // 命名空间的正则表达式
    running: Arc<Mutex<bool>>, // 标识 Poller 是否正在运行
    timer: Arc<Condvar>, // 条件变量，用于定时触发轮询
    thread: Mutex<Option<JoinHandle<()>>>, // Poller 的线程句柄
}

impl ActivePoller {
    pub fn new(interval: Duration, netns_regex: Option<Regex>) -> Self {
        Self {
            interval,
            version: Default::default(),
            entries: Default::default(),
            netns_regex: Arc::new(Mutex::new(netns_regex)),
            running: Default::default(),
            timer: Default::default(),
            thread: Default::default(),
        }
    }

    fn query(ns: &Vec<NsFile>) -> HashMap<NsFile, Vec<InterfaceInfo>> {
        match netns::interfaces_linked_with(&ns) {
            Ok(mut map) => {
                for (_, v) in map.iter_mut() {
                    v.sort_unstable();
                }
                map
            }
            Err(e) => {
                warn!("query namespace interfaces failed: {:?}", e);
                HashMap::new()
            }
        }
    }

    fn process(
        timer: Arc<Condvar>,
        running: Arc<Mutex<bool>>,
        version: Arc<AtomicU64>,
        entries: Arc<Mutex<HashMap<NsFile, Vec<InterfaceInfo>>>>,
        netns_regex: Arc<Mutex<Option<Regex>>>,
        timeout: Duration,
    ) {
        // 初始化
        // 总是查询根名称空间（/proc/1/ns/net）
        let mut nss = vec![NsFile::Root];
        if let Some(re) = &*netns_regex.lock().unwrap() {
            let mut extra_ns = netns::find_ns_files_by_regex(&re);
            extra_ns.sort_unstable();
            nss.extend(extra_ns);
        }
        let new_entries = Self::query(&nss);
        *entries.lock().unwrap() = new_entries;
        version.store(1);
        // 启动轮询循环
        loop {
            // 等待轮询间隔时间
            let (guard, _) = timer.wait_timeout(running.lock().unwrap(), timeout).unwrap();
            if !*guard {
                // Poller 停止运行，退出循环
                break;
            }
            // 更新命名空间和接口信息
            let mut nss = vec![NsFile::Root];
            if let Some(re) = &*netns_regex.lock().unwrap() {
                let mut extra_ns = netns::find_ns_files_by_regex(&re);
                extra_ns.sort_unstable();
                nss.extend(extra_ns);
            }
            let new_entries = Self::query(&nss);
            let mut expired_entries = Vec::new();
            {
                let mut entries = entries.lock().unwrap();
                for (ns, interfaces) in entries.iter_mut() {
                    if !new_entries.contains_key(ns) {
                        // 命名空间不再存在，增加过期计数器
                        if let Some(count) = ns.expire_count() {
                            if count >= ENTRY_EXPIRE_COUNT {
                                expired_entries.push(ns.clone());
                            } else {
                                ns.increase_expire_count();
                            }
                        } else {
                            ns.set_expire_count(1);
                        }
                    } else {
                        // 命名空间仍然存在，重置过期计数器
                        ns.reset_expire_count();
                    }
                    // 更新接口信息
                    if let Some(new_interfaces) = new_entries.get(ns) {
                        *interfaces = new_interfaces.clone();
                    }
                }
                // 移除过期的命名空间和接口信息
                for expired_entry in expired_entries {
                    entries.remove(&expired_entry);
                }
            }
            // 更新版本号
            let current_version = version.fetch_add(1, Ordering::Relaxed);
            debug!("ActivePoller: updated entries (version: {})", current_version);
        }
        info!("ActivePoller: stopped");
    }
}

impl Poller for ActivePoller {
    fn start(&mut self) {
        let mut running = self.running.lock().unwrap();
        if *running {
            // Poller 已经在运行，不执行重复启动
            return;
        }
        *running = true;
        let version = Arc::clone(&self.version);
        let entries = Arc::clone(&self.entries);
        let netns_regex = Arc::clone(&self.netns_regex);
        let timer = Arc::clone(&self.timer);
        let interval = self.interval;
        let running_flag = Arc::clone(&self.running);
        let thread = thread::spawn(move || {
            Self::process(timer, running_flag, version, entries, netns_regex, interval);
        });
        *self.thread.lock().unwrap() = Some(thread);
        trace!("ActivePoller: started");
    }

    fn stop(&mut self) {
        let mut running = self.running.lock().unwrap();
        if !*running {
            // Poller 已经停止运行，不执行重复停止
            return;
        }
        *running = false;
        self.timer.notify_one();
        if let Some(thread) = self.thread.lock().unwrap().take() {
            if let Err(e) = thread.join() {
                warn!("Failed to join ActivePoller thread: {:?}", e);
            }
        }
        trace!("ActivePoller: stopped");
    }

    fn get_entries(&self) -> HashMap<NsFile, Vec<InterfaceInfo>> {
        self.entries.lock().unwrap().clone()
    }
}
```

### api_watcher.cs:

```
/*这是一个基于 Rust 语言的 ActivePoller 实现，用于轮询命名空间和接口信息。

ActivePoller 是一个实现了 Poller trait 的结构体，用于轮询命名空间和接口信息。

interval: 轮询间隔。
version: AtomicU64 类型的原子变量，用于标识 entries 的更新。
entries: 一个包含命名空间和接口信息的 HashMap，存储了命名空间和对应的接口列表。
netns_regex: 命名空间的正则表达式，用于筛选需要轮询的命名空间。
running: 一个标识 Poller 是否正在运行的 Mutex<bool> 类型的变量。
timer: 一个用于定时触发轮询的 Condvar 类型的变量。
thread: Poller 的线程句柄。
ActivePoller 实现了 Poller trait 的方法，包括：

new: 创建一个 ActivePoller 实例。
start: 启动 Poller，开始轮询命名空间和接口信息。
stop: 停止 Poller 的轮询。
get_entries: 获取当前的命名空间和接口信息。

在实现中，ActivePoller 的 process 方法用于执行轮询逻辑。它会在启动时初始化命名空间和接口信息，然后进入一个循环，在循环中等待轮询间隔时间，更新命名空间和接口信息，并更新版本号。
如果 Poller 停止运行，循环退出。

start 方法会检查 Poller 是否已经在运行，如果是，则不执行重复启动。
否则，将标识 Poller 正在运行的 running 变量设为 true，创建一个线程来执行 process 方法，并将线程句柄保存在 thread 字段中。

stop 方法会检查 Poller 是否已经停止运行，如果是，则不执行重复停止。
否则，将标识 Poller 停止运行的 running 变量设为 false，通过调用 notify_one 方法通知等待的线程停止轮询，并等待线程结束。

get_entries 方法用于获取当前的命名空间和接口信息。它会获取 entries 字段的锁，并克隆出一个副本返回。

该实现在轮询过程中会查询指定的命名空间（包括根名称空间和符合正则表达式的额外命名空间），并将查询到的命名空间和对应的接口信息存储在 entries 字段中。
每次轮询开始时，会先更新命名空间和接口信息，并将版本号加一，以表示 entries 的更新。然后，根据新查询到的命名空间和接口信息更新已有的条目，如果某个命名空间不存在于新的查询结果中，
则增加其过期计数器，如果过期计数器达到最大值（ENTRY_EXPIRE_COUNT），则将该条目标记为过期，并从 entries 中移除。如果命名空间仍然存在于查询结果中，则重置其过期计数器。
最后，更新版本号，记录更新的次数。

整个轮询过程在一个无限循环中进行，直到 Poller 停止运行。当 Poller 停止运行时，会退出循环，并输出相应的日志。

该实现使用了多线程的方式执行轮询逻辑，通过线程间的锁和条件变量来实现线程同步。这样可以使得轮询过程在后台异步执行，不会阻塞主线程。

该ActivePoller用于轮询命名空间和接口信息，并根据查询结果进行更新和维护。它提供了启动、停止和获取当前命名空间和接口信息的接口，可以方便地与其他组件进行集成和使用。*/

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use log::{debug, info, log_enabled, trace, warn, Level};
use regex::Regex;

use super::Poller;
use public::netns::{self, InterfaceInfo, NsFile};

const ENTRY_EXPIRE_COUNT: u8 = 3; // 条目过期计数器的最大值

#[derive(Debug, Default)]
pub struct ActivePoller {
    interval: Duration, // 轮询间隔
    version: Arc<AtomicU64>, // 版本号，用于标识 entries 的更新
    entries: Arc<Mutex<HashMap<NsFile, Vec<InterfaceInfo>>>>, // 存储命名空间和接口信息的映射
    netns_regex: Arc<Mutex<Option<Regex>>>, // 命名空间的正则表达式
    running: Arc<Mutex<bool>>, // 标识 Poller 是否正在运行
    timer: Arc<Condvar>, // 条件变量，用于定时触发轮询
    thread: Mutex<Option<JoinHandle<()>>>, // Poller 的线程句柄
}

impl ActivePoller {
    pub fn new(interval: Duration, netns_regex: Option<Regex>) -> Self {
        Self {
            interval,
            version: Default::default(),
            entries: Default::default(),
            netns_regex: Arc::new(Mutex::new(netns_regex)),
            running: Default::default(),
            timer: Default::default(),
            thread: Default::default(),
        }
    }

    fn query(ns: &Vec<NsFile>) -> HashMap<NsFile, Vec<InterfaceInfo>> {
        match netns::interfaces_linked_with(&ns) {
            Ok(mut map) => {
                for (_, v) in map.iter_mut() {
                    v.sort_unstable();
                }
                map
            }
            Err(e) => {
                warn!("query namespace interfaces failed: {:?}", e);
                HashMap::new()
            }
        }
    }

    fn process(
        timer: Arc<Condvar>,
        running: Arc<Mutex<bool>>,
        version: Arc<AtomicU64>,
        entries: Arc<Mutex<HashMap<NsFile, Vec<InterfaceInfo>>>>,
        netns_regex: Arc<Mutex<Option<Regex>>>,
        timeout: Duration,
    ) {
        // 初始化
        // 总是查询根名称空间（/proc/1/ns/net）
        let mut nss = vec![NsFile::Root];
        if let Some(re) = &*netns_regex.lock().unwrap() {
            let mut extra_ns = netns::find_ns_files_by_regex(&re);
            extra_ns.sort_unstable();
            nss.extend(extra_ns);
        }
        let new_entries = Self::query(&nss);
        *entries.lock().unwrap() = new_entries;
        version.store(1);
        // 启动轮询循环
        loop {
            // 等待轮询间隔时间
            let (guard, _) = timer.wait_timeout(running.lock().unwrap(), timeout).unwrap();
            if !*guard {
                // Poller 停止运行，退出循环
                break;
            }
            // 更新命名空间和接口信息
            let mut nss = vec![NsFile::Root];
            if let Some(re) = &*netns_regex.lock().unwrap() {
                let mut extra_ns = netns::find_ns_files_by_regex(&re);
                extra_ns.sort_unstable();
                nss.extend(extra_ns);
            }
            let new_entries = Self::query(&nss);
            let mut expired_entries = Vec::new();
            {
                let mut entries = entries.lock().unwrap();
                for (ns, interfaces) in entries.iter_mut() {
                    if !new_entries.contains_key(ns) {
                        // 命名空间不再存在，增加过期计数器
                        if let Some(count) = ns.expire_count() {
                            if count >= ENTRY_EXPIRE_COUNT {
                                expired_entries.push(ns.clone());
                            } else {
                                ns.increase_expire_count();
                            }
                        } else {
                            ns.set_expire_count(1);
                        }
                    } else {
                        // 命名空间仍然存在，重置过期计数器
                        ns.reset_expire_count();
                    }
                    // 更新接口信息
                    if let Some(new_interfaces) = new_entries.get(ns) {
                        *interfaces = new_interfaces.clone();
                    }
                }
                // 移除过期的命名空间和接口信息
                for expired_entry in expired_entries {
                    entries.remove(&expired_entry);
                }
            }
            // 更新版本号
            let current_version = version.fetch_add(1, Ordering::Relaxed);
            debug!("ActivePoller: updated entries (version: {})", current_version);
        }
        info!("ActivePoller: stopped");
    }
}

impl Poller for ActivePoller {
    fn start(&mut self) {
        let mut running = self.running.lock().unwrap();
        if *running {
            // Poller 已经在运行，不执行重复启动
            return;
        }
        *running = true;
        let version = Arc::clone(&self.version);
        let entries = Arc::clone(&self.entries);
        let netns_regex = Arc::clone(&self.netns_regex);
        let timer = Arc::clone(&self.timer);
        let interval = self.interval;
        let running_flag = Arc::clone(&self.running);
        let thread = thread::spawn(move || {
            Self::process(timer, running_flag, version, entries, netns_regex, interval);
        });
        *self.thread.lock().unwrap() = Some(thread);
        trace!("ActivePoller: started");
    }

    fn stop(&mut self) {
        let mut running = self.running.lock().unwrap();
        if !*running {
            // Poller 已经停止运行，不执行重复停止
            return;
        }
        *running = false;
        self.timer.notify_one();
        if let Some(thread) = self.thread.lock().unwrap().take() {
            if let Err(e) = thread.join() {
                warn!("Failed to join ActivePoller thread: {:?}", e);
            }
        }
        trace!("ActivePoller: stopped");
    }

    fn get_entries(&self) -> HashMap<NsFile, Vec<InterfaceInfo>> {
        self.entries.lock().unwrap().clone()
    }
}
```

### crd.cs:

```
/*这段代码主要定义了一些自定义资源类型，用于扩展 Kubernetes API。
每个自定义资源类型都实现了 CustomResource trait 和一些其他相关的 trait，以及一些自定义的字段和方法。
这些自定义资源类型可以在 Kubernetes 中使用，并与其他 Kubernetes 资源进行交互和管理。

这些自定义资源类型是用于扩展 Kubernetes API，以满足特定应用程序或业务领域的需求。
通过创建自定义资源类型，用户可以定义自己的资源对象，并在 Kubernetes 中以类似于内置资源（例如 Pod、Service、Deployment 等）的方式进行管理和操作。

自定义资源类型的主要目的是引入新的概念和功能，以适应特定领域或应用程序的需求。它们可以代表任何与应用程序相关的实体、服务或配置，允许用户定义自己的 API 结构和行为。

通过创建自定义资源类型，用户可以在 Kubernetes 中使用自己定义的资源对象，并利用 Kubernetes 提供的强大功能，如自动伸缩、监控、日志记录和调度等。
这样可以更好地集成自定义应用程序和工作负载，并利用 Kubernetes 生态系统的丰富功能和工具。

总的来说，自定义资源类型允许用户根据自己的需求扩展 Kubernetes 平台，使其更适应特定的应用场景和业务需求，从而提供更灵活和定制化的解决方案。*/

use std::collections::BTreeMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::resource_watcher::Trimmable;

// 定义了名为 `pingan` 的模块
pub mod pingan {
    use super::*;

    use k8s_openapi::api::core::v1::ServicePort;

    // 定义了名为 `ServiceRuleSpec` 的结构体，它实现了 `CustomResource` trait
    #[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
    #[kube(
        group = "crd.pingan.org",
        version = "v1alpha1",
        kind = "ServiceRule",
        namespaced
    )]
    #[serde(rename_all = "camelCase")]
    pub struct ServiceRuleSpec {
        // 定义了 `cluster_ip` 字段，类型为可选的字符串
        #[serde(rename = "clusterIP")]
        pub cluster_ip: Option<String>,
        // 定义了 `ports` 字段，类型为可选的 `Vec<ServicePort>` 向量
        pub ports: Option<Vec<ServicePort>>,
        // 定义了 `selector` 字段，类型为可选的 `BTreeMap<String, String>` 字典
        pub selector: Option<BTreeMap<String, String>>,
        // 定义了 `type_` 字段，类型为可选的字符串
        pub type_: Option<String>,
    }

    // `ServiceRule` 结构体实现了 `Trimmable` trait
    impl Trimmable for ServiceRule {
        // 实现了 `trim` 方法，用于进行修剪操作
        fn trim(mut self) -> Self {
            // 获取名称，如果存在则使用名称，否则使用空字符串
            let name = if let Some(name) = self.metadata.name.as_ref() {
                name
            } else {
                ""
            };
            // 创建一个新的 `ServiceRule` 对象，并设置其元数据（metadata）
            let mut sr = Self::new(name, self.spec);
            sr.metadata = ObjectMeta {
                uid: self.metadata.uid.take(),
                name: self.metadata.name.take(),
                namespace: self.metadata.namespace.take(),
                annotations: self.metadata.annotations.take(),
                labels: self.metadata.labels.take(),
                ..Default::default()
            };
            sr
        }
    }
}

// 定义了名为 `kruise` 的模块
pub mod kruise {
    use super::*;

    use k8s_openapi::{
        api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::LabelSelector,
    };

    // 定义了名为 `CloneSetSpec` 的结构体，它实现了 `CustomResource` trait
    #[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
    #[kube(
        group = "apps.kruise.io",
        version = "v1alpha1",
        kind = "CloneSet",
        namespaced
    )]
    #[serde(rename_all = "camelCase")]
    pub struct CloneSetSpec {
        // 定义了 `relicas` 字段，类型为可选的 i32
        pub relicas: Option<i32>,
        // 定义了 `selector` 字段，类型为 `LabelSelector`
        pub selector: LabelSelector,
        // 定义了 `template` 字段，类型为 `PodTemplateSpec`
        pub template: PodTemplateSpec,
    }

    // `CloneSet` 结构体实现了 `Trimmable` trait
    impl Trimmable for CloneSet {
        // 实现了 `trim` 方法，用于进行修剪操作
        fn trim(mut self) -> Self {
            // 获取名称，如果存在则使用名称，否则使用空字符串
            let name = if let Some(name) = self.metadata.name.as_ref() {
                name
            } else {
                ""
            };
            // 创建一个新的 `CloneSet` 对象，并设置其元数据（metadata）
            let mut cs = Self::new(name, self.spec);
            cs.metadata = ObjectMeta {
                uid: self.metadata.uid.take(),
                name: self.metadata.name.take(),
                namespace: self.metadata.namespace.take(),
                labels: self.metadata.labels.take(),
                ..Default::default()
            };
            cs
        }
    }

    // 定义了名为 `StatefulSetSpec` 的结构体，它实现了 `CustomResource` trait
    #[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
    #[kube(
        group = "apps.kruise.io",
        version = "v1beta1",
        kind = "StatefulSet",
        namespaced
    )]
    #[serde(rename_all = "camelCase")]
    pub struct StatefulSetSpec {
        // 定义了 `relicas` 字段，类型为可选的 i32
        pub relicas: Option<i32>,
        // 定义了 `selector` 字段，类型为 `LabelSelector`
        pub selector: LabelSelector,
        // 定义了 `template` 字段，类型为 `PodTemplateSpec`
        pub template: PodTemplateSpec,
    }

    // `StatefulSet` 结构体实现了 `Trimmable` trait
    impl Trimmable for StatefulSet {
        // 实现了 `trim` 方法，用于进行修剪操作
        fn trim(mut self) -> Self {
            // 获取名称，如果存在则使用名称，否则使用空字符串
            let name = if let Some(name) = self.metadata.name.as_ref() {
                name
            } else {
                ""
            };
            // 创建一个新的 `StatefulSet` 对象，并设置其元数据（metadata）
            let mut ss = Self::new(name, self.spec);
            ss.metadata = ObjectMeta {
                uid: self.metadata.uid.take(),
                name: self.metadata.name.take(),
                namespace: self.metadata.namespace.take(),
                labels: self.metadata.labels.take(),
                ..Default::default()
            };
            ss
        }
    }
}
```

### passive_poller.rs:

```
    /*从fn process函数开始才是一个主要的处理函数，用于接收和处理网络数据包。
    它采用了多个参数，包括一个标识运行状态的原子布尔值(running)，超时时间(expire_timeout)，版本号(version)，数据包条目(entries)，和捕获模式(tap_mode)。
    函数的主要逻辑是通过不同的捕获模式初始化并设置捕获引擎(engine)，然后在一个循环中接收数据包并进行处理。
    处理过程包括移除超时的记录、解析数据包类型和内容，更新数据包条目等操作。*/

use std::{
    cmp,
    collections::HashSet,
    ffi::CString,
    fmt,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime},
};

use arc_swap::access::Access;
use libc::RT_SCOPE_LINK;
use log::{debug, error, info, log_enabled, warn, Level};
use pnet::packet::icmpv6::Icmpv6Types;
use regex::Regex;

use super::Poller;
use crate::{
    common::{
        ARP_SPA_OFFSET, ETH_TYPE_LEN, ETH_TYPE_OFFSET, FIELD_OFFSET_SA, ICMPV6_TYPE_OFFSET,
        ICMPV6_TYPE_SIZE, IPV4_ADDR_LEN, IPV6_ADDR_LEN, IPV6_FRAGMENT_LEN, IPV6_PROTO_LEN,
        IPV6_PROTO_OFFSET, IPV6_SRC_OFFSET, MAC_ADDR_LEN, VLAN_HEADER_SIZE,
    },
    config::handler::PlatformAccess,
    dispatcher::{
        af_packet::{bpf::*, Options, Tpacket},
        recv_engine::{
            RecvEngine, DEFAULT_BLOCK_SIZE, FRAME_SIZE_MAX, FRAME_SIZE_MIN, POLL_TIMEOUT,
        },
    },
};

use public::{
    bytes::read_u16_be,
    enums::{EthernetType, IpProtocol},
    error::Error,
    netns::{InterfaceInfo, NsFile},
    proto::trident::TapMode,
    utils::net::{addr_list, MacAddr},
};

const LINUX_SLL_PACKET_TYPE_OUT_GONING: u32 = 4;
const MINUTE: Duration = Duration::from_secs(60);

pub struct PassivePoller {
    expire_timeout: Duration,
    version: Arc<AtomicU64>,
    entries: Arc<Mutex<Vec<PassiveEntry>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    config: PlatformAccess,
}

impl PassivePoller {
    pub fn new(interval: Duration, config: PlatformAccess) -> Self {
        Self {
            config,
            expire_timeout: interval,
            version: Default::default(),
            running: Default::default(),
            thread: Default::default(),
            entries: Default::default(),
        }
    }

    // 排除掉有非link类型IP的接口，这些不会作为tap interface
    // Exclude interfaces with non-link type IPs, these will not be used as tap interfaces
    fn get_ignored_interface_indice() -> HashSet<u32> {
        let mut ignored = HashSet::new();

        let Ok(addrs) = addr_list() else { return ignored; };
        for addr in addrs {
            if addr.scope != RT_SCOPE_LINK {
                ignored.insert(addr.if_index);
            }
        }

        // HashSet Debug trait is {a, b, c, d} better than handwrite formatted string
        debug!("ignore tap interfaces with id in {ignored:?}");

        ignored
    }

    fn get_bpf() -> Vec<RawInstruction> {
        let bpf = vec![
            // 对于宿主命名空间的接口来看，容器发出来的流量是inbound
            // From the perspective of the interface of the host namespace, the traffic sent by the container is inbound
            BpfSyntax::LoadExtension(LoadExtension {
                num: Extension::ExtType,
            }),
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpEqual,
                val: LINUX_SLL_PACKET_TYPE_OUT_GONING,
                skip_false: 1,
                skip_true: 0,
            }),
            BpfSyntax::RetConstant(RetConstant { val: 0 }),
            // 跳过VLAN
            // skip VLAN packet
            BpfSyntax::LoadAbsolute(LoadAbsolute {
                off: ETH_TYPE_OFFSET as u32,
                size: ETH_TYPE_LEN as u32,
            }),
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpNotEqual,
                val: u16::from(EthernetType::Dot1Q) as u32,
                skip_false: 0,
                skip_true: 2,
            }),
            BpfSyntax::LoadConstant(LoadConstant {
                dst: Register::RegX,
                val: VLAN_HEADER_SIZE as u32,
            }),
            BpfSyntax::LoadIndirect(LoadIndirect {
                off: ETH_TYPE_OFFSET as u32,
                size: ETH_TYPE_LEN as u32,
            }),
            // ARP
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpEqual,
                val: u16::from(EthernetType::Arp) as u32,
                skip_false: 1,
                skip_true: 0,
            }),
            BpfSyntax::RetConstant(RetConstant { val: 64 }),
            // IPv6
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpEqual,
                val: u16::from(EthernetType::Ipv6) as u32,
                skip_true: 1,
                skip_false: 0,
            }),
            BpfSyntax::RetConstant(RetConstant { val: 0 }),
            // IPv6 next header
            BpfSyntax::LoadIndirect(LoadIndirect {
                off: IPV6_PROTO_OFFSET as u32,
                size: IPV6_PROTO_LEN as u32,
            }),
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpEqual,
                val: u8::from(IpProtocol::Icmpv6) as u32,
                skip_true: 8,
                skip_false: 0,
            }),
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpEqual,
                val: u8::from(IpProtocol::Ipv6Fragment) as u32,
                skip_true: 1,
                skip_false: 0,
            }),
            BpfSyntax::RetConstant(RetConstant { val: 0 }),
            // skip fragment header
            BpfSyntax::Txa(Txa),
            BpfSyntax::ALUOpConstant(ALUOpConstant {
                op: ALU_OP_ADD,
                val: IPV6_FRAGMENT_LEN as u32,
            }),
            BpfSyntax::Txa(Txa),
            BpfSyntax::LoadIndirect(LoadIndirect {
                off: IPV6_PROTO_OFFSET as u32,
                size: IPV6_PROTO_LEN as u32,
            }),
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpEqual,
                val: u8::from(IpProtocol::Icmpv6) as u32,
                skip_true: 1,
                skip_false: 0,
            }),
            BpfSyntax::RetConstant(RetConstant { val: 0 }),
            // neighbour advertisement
            BpfSyntax::LoadIndirect(LoadIndirect {
                off: ICMPV6_TYPE_OFFSET as u32,
                size: ICMPV6_TYPE_SIZE as u32,
            }),
            BpfSyntax::JumpIf(JumpIf {
                cond: JumpTest::JumpEqual,
                val: Icmpv6Types::NeighborAdvert.0 as u32,
                skip_true: 0,
                skip_false: 1,
            }),
            BpfSyntax::RetConstant(RetConstant { val: 128 }),
            BpfSyntax::RetConstant(RetConstant { val: 0 }),
        ];

        bpf.into_iter().map(|ins| ins.to_instruction()).collect()
    }

    fn process(
        running: Arc<AtomicBool>,
        expire_timeout: Duration,
        version: Arc<AtomicU64>,
        entries: Arc<Mutex<Vec<PassiveEntry>>>,
        tap_mode: TapMode,
    ) {
        let mut engine = match tap_mode {
            TapMode::Local | TapMode::Mirror | TapMode::Analyzer => {
                let afp = Options {
                    frame_size: if tap_mode == TapMode::Analyzer {
                        FRAME_SIZE_MIN as u32
                    } else {
                        FRAME_SIZE_MAX as u32
                    },
                    block_size: DEFAULT_BLOCK_SIZE as u32,
                    num_blocks: 128,
                    poll_timeout: POLL_TIMEOUT.as_nanos() as isize,
                    ..Default::default()
                };
                info!("Afpacket init with {afp:?}");
                RecvEngine::AfPacket(Tpacket::new(afp).unwrap())
            }
            _ => {
                error!("construct RecvEngine error: TapMode({tap_mode:?}) not support");
                return;
            }
        };
        if let Err(e) = engine.set_bpf(Self::get_bpf(), &CString::new("").unwrap()) {
            error!("RecvEngine set bpf error: {e}");
            return;
        }

        let mut last_version = version.load(Ordering::Relaxed);
        let mut last_version_log = SystemTime::now();
        let mut last_expire = SystemTime::now();
        let mut ignored_indice = Self::get_ignored_interface_indice();
        while running.load(Ordering::Relaxed) {
            // The lifecycle of the packet will end before the next call to recv.
            let packet = match unsafe { engine.recv() } {
                Ok(p) => p,
                Err(Error::Timeout) => continue,
                Err(e) => {
                    warn!("capture packet failed: {}", e);
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
            };
            let now = SystemTime::now();
            // 每分钟移除超时的记录
            // Remove timed out records every minute
            if now.duration_since(last_expire).unwrap() > MINUTE {
                ignored_indice = Self::get_ignored_interface_indice();
                let mut entries_gurad = entries.lock().unwrap();
                let old_len = entries_gurad.len();
                entries_gurad
                    .retain(|e| now.duration_since(e.last_seen).unwrap() <= expire_timeout);

                if entries_gurad.len() != old_len {
                    version.fetch_add(1, Ordering::Relaxed);
                }
                last_expire = now;
            }

            if ignored_indice.contains(&(packet.if_index as u32)) {
                continue;
            }

            let packet_len = packet.data.len();
            let packet_data = &packet.data;
            if packet_len < ETH_TYPE_OFFSET + 2 * VLAN_HEADER_SIZE + 2 {
                // 22
                debug!("ignore short packet, size={packet_len}");
                continue;
            }

            let mut eth_type = read_u16_be(&packet_data[ETH_TYPE_OFFSET..]);
            let mut extra_offset = 0;
            if eth_type == EthernetType::Dot1Q {
                extra_offset += VLAN_HEADER_SIZE;
                eth_type = read_u16_be(&packet_data[ETH_TYPE_OFFSET + extra_offset..]);
                if eth_type == EthernetType::Dot1Q {
                    extra_offset += VLAN_HEADER_SIZE;
                    eth_type = read_u16_be(&packet_data[ETH_TYPE_OFFSET + extra_offset..]);
                }
            }

            let eth_type = match EthernetType::try_from(eth_type) {
                Ok(e) => e,
                Err(e) => {
                    debug!("parse packet eth_type failed: {e}");
                    continue;
                }
            };

            let entry = match eth_type {
                EthernetType::Arp => {
                    if packet_len < ARP_SPA_OFFSET + extra_offset + 4 {
                        debug!("ignore short arp packet, size={packet_len}");
                        continue;
                    }

                    let so = ARP_SPA_OFFSET + extra_offset;
                    PassiveEntry {
                        last_seen: now,
                        tap_index: packet.if_index as u32,
                        mac: MacAddr::try_from(
                            &packet_data[FIELD_OFFSET_SA..FIELD_OFFSET_SA + MAC_ADDR_LEN],
                        )
                        .unwrap(),
                        ip: IpAddr::from(
                            *<&[u8; 4]>::try_from(&packet_data[so..so + IPV4_ADDR_LEN]).unwrap(),
                        ),
                    }
                }
                EthernetType::Ipv6 => {
                    if packet_len < IPV6_PROTO_OFFSET + extra_offset + 1 {
                        debug!("ignore short ipv6 packet, size={packet_len}");
                        continue;
                    }

                    let mut protocol = packet_data[IPV6_PROTO_OFFSET + extra_offset];
                    if protocol == IpProtocol::Ipv6 {
                        extra_offset += IPV6_FRAGMENT_LEN;
                        protocol = packet_data[IPV6_PROTO_OFFSET + extra_offset];
                    }
                    if packet_len < ICMPV6_TYPE_OFFSET + extra_offset + 1 {
                        debug!("ignore short icmpv6 packet, size={packet_len}");
                        continue;
                    }
                    if protocol != IpProtocol::Icmpv6
                        || packet_data[ICMPV6_TYPE_OFFSET + extra_offset]
                            != Icmpv6Types::NeighborAdvert.0
                    {
                        continue;
                    }

                    let so = IPV6_SRC_OFFSET + extra_offset;
                    PassiveEntry {
                        last_seen: now,
                        tap_index: packet.if_index as u32,
                        mac: MacAddr::try_from(
                            &packet_data[FIELD_OFFSET_SA..FIELD_OFFSET_SA + MAC_ADDR_LEN],
                        )
                        .unwrap(),
                        ip: IpAddr::from(
                            *<&[u8; 16]>::try_from(&packet_data[so..so + IPV6_ADDR_LEN]).unwrap(),
                        ),
                    }
                }
                _ => continue,
            };

            {
                let mut entries = entries.lock().unwrap();
                let index = entries.partition_point(|x| x < &entry);
                if index >= entries.len() || !entries[index].eq(&entry) {
                    entries.insert(index, entry);
                    version.fetch_add(1, Ordering::Relaxed);
                } else {
                    entries[index].last_seen = entry.last_seen;
                }
            }
            let new_version = version.load(Ordering::Relaxed);
            if last_version != new_version {
                if now.duration_since(last_version_log).unwrap() > MINUTE {
                    info!("kubernetes poller updated to version {new_version}");
                    last_version_log = now;
                    if log_enabled!(Level::Debug) {
                        for entry in entries.lock().unwrap().iter() {
                            debug!("{entry}");
                        }
                    }
                }
                last_version = new_version;
            }
        }
    }
}

impl Poller for PassivePoller {
    fn get_version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    fn get_interface_info_in(&self, _: &NsFile) -> Option<Vec<InterfaceInfo>> {
        Some(self.get_interface_info())
    }

    /*这个函数用于获取网络接口的信息。它从数据包条目(entries)中提取并组织出网络接口的相关信息，包括接口索引、MAC地址、IP地址等。返回一个包含多个接口信息的向量。*/
    fn get_interface_info(&self) -> Vec<InterfaceInfo> {
        let entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return vec![];
        }

        let mut info_slice = vec![];
        let mut info = InterfaceInfo {
            tap_idx: entries[0].tap_index,
            mac: entries[0].mac,
            ips: vec![entries[0].ip],
            device_id: "1".to_string(),
            ..Default::default()
        };
        for entry in entries.iter().skip(1) {
            if entry.tap_index == info.tap_idx && entry.mac == info.mac {
                info.ips.push(entry.ip);
            } else {
                info_slice.push(info.clone());
                info = InterfaceInfo {
                    tap_idx: entry.tap_index,
                    mac: entry.mac,
                    ips: vec![entry.ip],
                    device_id: "1".to_string(),
                    ..Default::default()
                };
            }
        }

        info_slice.push(info);
        info_slice
    }

    fn set_netns_regex(&self, _: Option<Regex>) {}

    //这个函数用于启动被动型网络数据包捕获和处理功能。它设置运行状态为启动，并创建一个线程来调用process()函数进行数据包处理。
    fn start(&self) {
        if self.running.swap(true, Ordering::Relaxed) {
            return;
        }

        let expire_timeout = self.expire_timeout;
        let running = self.running.clone();
        let version = self.version.clone();
        let entries = self.entries.clone();
        let tap_mode = self.config.load().tap_mode;
        let handle = thread::Builder::new()
            .name("kubernetes-poller".to_owned())
            .spawn(move || Self::process(running, expire_timeout, version, entries, tap_mode))
            .unwrap();
        self.thread.lock().unwrap().replace(handle);
        info!("kubernetes passive poller started");
    }

    //stop(): 这个函数用于停止被动型网络数据包捕获和处理功能。它设置运行状态为停止，并等待线程结束。
    fn stop(&self) {
        if !self.running.swap(false, Ordering::Relaxed) {
            return;
        }

        if let Some(handle) = self.thread.lock().unwrap().take() {
            let _ = handle.join();
        }

        info!("kubernetes passive poller stopped");
    }
}

struct PassiveEntry {
    tap_index: u32,
    mac: MacAddr,
    ip: IpAddr,
    last_seen: SystemTime,
}

impl fmt::Display for PassiveEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} {}", self.tap_index, self.mac, self.ip)
    }
}

impl PartialEq for PassiveEntry {
    fn eq(&self, other: &Self) -> bool {
        self.tap_index.eq(&other.tap_index) && self.mac.eq(&other.mac) && self.ip.eq(&other.ip)
    }
}

impl Eq for PassiveEntry {}

impl PartialOrd for PassiveEntry {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PassiveEntry {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let tap_index_ordering = self.tap_index.cmp(&other.tap_index);
        if tap_index_ordering != cmp::Ordering::Equal {
            return tap_index_ordering;
        }

        let mac_ordering = self.mac.cmp(&other.mac);
        if mac_ordering != cmp::Ordering::Equal {
            return mac_ordering;
        }

        self.ip.cmp(&other.ip)
    }
}
```

### resource_watcher:

```
/*这段代码的作用是提供了一个可定制的资源监视功能，用于监视和操作 Kubernetes 资源。
trim 函数：这是 Trimmable trait 中定义的函数，用于对资源对象进行修剪操作。不同的资源类型会实现自己的 trim 函数来裁剪资源对象的字段，以减少其大小和复杂性。

new_watcher 函数：这是 ResourceWatcherFactory 结构体中的方法之一。
它接收资源类型和命名空间作为参数，根据传入的参数创建相应类型的资源监视器，并返回一个 GenericResourceWatcher 枚举类型的实例。

new_watcher_inner 函数：也是 ResourceWatcherFactory 结构体中的方法之一。
它根据传入的资源类型和配置创建具体的资源监视器。该函数使用了泛型参数 K，并对其施加了约束条件，包括实现了 KubeResource trait 和 Trimmable trait，并提供了默认的 DynamicType。
该函数创建一个 ResourceWatcher<K> 实例，并将其注册到一个统计收集器中。

trim 函数用于资源对象的修剪，new_watcher 函数用于创建资源监视器，而 new_watcher_inner 函数负责根据资源类型和配置创建具体的资源监视器，并进行注册操作。*/

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    io::{self, Write},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant, SystemTime},
};

use enum_dispatch::enum_dispatch;
use flate2::{write::ZlibEncoder, Compression};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::{
    api::{
        apps::v1::{
            DaemonSet, DaemonSetSpec, Deployment, DeploymentSpec, ReplicaSet, ReplicaSetSpec,
            StatefulSet, StatefulSetSpec,
        },
        core::v1::{
            Container, ContainerStatus, Namespace, Node, NodeSpec, NodeStatus, Pod, PodSpec,
            PodStatus, ReplicationController, ReplicationControllerSpec, Service, ServiceSpec,
        },
        extensions, networking,
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::ListParams,
    runtime::{self, watcher::Event},
    Api, Client, Resource as KubeResource,
};
use log::{debug, info, trace, warn};
use openshift_openapi::api::route::v1::Route;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use tokio::{runtime::Handle, sync::Mutex, task::JoinHandle, time};

use super::crd::{
    kruise::{CloneSet, StatefulSet as KruiseStatefulSet},
    pingan::ServiceRule,
};
use crate::utils::stats::{
    self, Countable, Counter, CounterType, CounterValue, RefCountable, StatsOption,
};

const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);
const SLEEP_INTERVAL: Duration = Duration::from_secs(5);
const SPIN_INTERVAL: Duration = Duration::from_millis(100);

#[enum_dispatch]
pub trait Watcher {
    fn start(&self) -> Option<JoinHandle<()>>;
    fn error(&self) -> Option<String>;
    fn entries(&self) -> Vec<Vec<u8>>;
    fn pb_name(&self) -> &str;
    fn version(&self) -> u64;
    fn ready(&self) -> bool;
}

#[enum_dispatch(Watcher)]
#[derive(Clone)]
pub enum GenericResourceWatcher {
    Node(ResourceWatcher<Node>),
    Namespace(ResourceWatcher<Namespace>),
    Service(ResourceWatcher<Service>),
    Deployment(ResourceWatcher<Deployment>),
    Pod(ResourceWatcher<Pod>),
    StatefulSet(ResourceWatcher<StatefulSet>),
    DaemonSet(ResourceWatcher<DaemonSet>),
    ReplicationController(ResourceWatcher<ReplicationController>),
    ReplicaSet(ResourceWatcher<ReplicaSet>),
    V1Ingress(ResourceWatcher<networking::v1::Ingress>),
    V1beta1Ingress(ResourceWatcher<networking::v1beta1::Ingress>),
    ExtV1beta1Ingress(ResourceWatcher<extensions::v1beta1::Ingress>),
    Route(ResourceWatcher<Route>),

    // CRDs
    ServiceRule(ResourceWatcher<ServiceRule>),
    CloneSet(ResourceWatcher<CloneSet>),
    KruiseStatefulSet(ResourceWatcher<KruiseStatefulSet>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupVersion {
    pub group: &'static str,
    pub version: &'static str,
}

impl fmt::Display for GroupVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.group, self.version)
    }
}

#[derive(Clone, Debug)]
pub struct Resource {
    pub name: &'static str,
    pub pb_name: &'static str,
    // supported group versions ordered by priority
    pub group_versions: Vec<GroupVersion>,
    // group version to use
    pub selected_gv: Option<GroupVersion>,
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.selected_gv {
            Some(gv) => write!(f, "{}/{}", gv, self.name),
            None => write!(f, "{}: {:?}", self.name, self.group_versions),
        }
    }
}

pub fn default_resources() -> Vec<Resource> {
    vec![
        Resource {
            name: "namespaces",
            pb_name: "*v1.Namespace",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "nodes",
            pb_name: "*v1.Node",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "pods",
            pb_name: "*v1.Pod",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "replicationcontrollers",
            pb_name: "*v1.ReplicationController",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "services",
            pb_name: "*v1.Service",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "daemonsets",
            pb_name: "*v1.DaemonSet",
            group_versions: vec![GroupVersion {
                group: "apps",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "deployments",
            pb_name: "*v1.Deployment",
            group_versions: vec![GroupVersion {
                group: "apps",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "replicasets",
            pb_name: "*v1.ReplicaSet",
            group_versions: vec![GroupVersion {
                group: "apps",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "statefulsets",
            pb_name: "*v1.StatefulSet",
            group_versions: vec![GroupVersion {
                group: "apps",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "ingresses",
            pb_name: "*v1.Ingress",
            group_versions: vec![
                GroupVersion {
                    group: "networking.k8s.io",
                    version: "v1",
                },
                GroupVersion {
                    group: "networking.k8s.io",
                    version: "v1beta1",
                },
                GroupVersion {
                    group: "extensions",
                    version: "v1beta1",
                },
            ],
            selected_gv: None,
        },
    ]
}

pub fn supported_resources() -> Vec<Resource> {
    vec![
        Resource {
            name: "namespaces",
            pb_name: "*v1.Namespace",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "nodes",
            pb_name: "*v1.Node",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "pods",
            pb_name: "*v1.Pod",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "replicationcontrollers",
            pb_name: "*v1.ReplicationController",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "services",
            pb_name: "*v1.Service",
            group_versions: vec![GroupVersion {
                group: "core",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "daemonsets",
            pb_name: "*v1.DaemonSet",
            group_versions: vec![GroupVersion {
                group: "apps",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "deployments",
            pb_name: "*v1.Deployment",
            group_versions: vec![GroupVersion {
                group: "apps",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "replicasets",
            pb_name: "*v1.ReplicaSet",
            group_versions: vec![GroupVersion {
                group: "apps",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "statefulsets",
            pb_name: "*v1.StatefulSet",
            group_versions: vec![
                GroupVersion {
                    group: "apps",
                    version: "v1",
                },
                GroupVersion {
                    group: "apps.kruise.io",
                    version: "v1beta1",
                },
            ],
            selected_gv: None,
        },
        Resource {
            name: "ingresses",
            pb_name: "*v1.Ingress",
            group_versions: vec![
                GroupVersion {
                    group: "networking.k8s.io",
                    version: "v1",
                },
                GroupVersion {
                    group: "networking.k8s.io",
                    version: "v1beta1",
                },
                GroupVersion {
                    group: "extensions",
                    version: "v1beta1",
                },
            ],
            selected_gv: None,
        },
        Resource {
            name: "routes",
            pb_name: "*v1.Ingress",
            group_versions: vec![GroupVersion {
                group: "route.openshift.io",
                version: "v1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "servicerules",
            pb_name: "*v1.ServiceRule",
            group_versions: vec![GroupVersion {
                group: "crd.pingan.org",
                version: "v1alpha1",
            }],
            selected_gv: None,
        },
        Resource {
            name: "clonesets",
            pb_name: "*v1.CloneSet",
            group_versions: vec![GroupVersion {
                group: "apps.kruise.io",
                version: "v1alpha1",
            }],
            selected_gv: None,
        },
    ]
}

#[derive(Default)]
pub struct WatcherCounter {
    list_count: AtomicU32,
    list_length: AtomicU32,
    list_cost_time_sum: AtomicU64, // ns
    list_error: AtomicU32,
    watch_applied: AtomicU32,
    watch_deleted: AtomicU32,
    watch_restarted: AtomicU32,
}

impl RefCountable for WatcherCounter {
    fn get_counters(&self) -> Vec<Counter> {
        let list_count = self.list_count.swap(0, Ordering::Relaxed);
        let list_avg_cost_time = self
            .list_cost_time_sum
            .swap(0, Ordering::Relaxed)
            .checked_div(list_count as u64)
            .unwrap_or_default();
        let list_avg_length = self
            .list_length
            .swap(0, Ordering::Relaxed)
            .checked_div(list_count)
            .unwrap_or_default();
        vec![
            (
                "list_avg_length",
                CounterType::Gauged,
                CounterValue::Unsigned(list_avg_length as u64),
            ),
            (
                "list_avg_cost_time",
                CounterType::Counted,
                CounterValue::Unsigned(list_avg_cost_time),
            ),
            (
                "list_error",
                CounterType::Gauged,
                CounterValue::Unsigned(self.list_error.swap(0, Ordering::Relaxed) as u64),
            ),
            (
                "watch_applied",
                CounterType::Gauged,
                CounterValue::Unsigned(self.watch_applied.swap(0, Ordering::Relaxed) as u64),
            ),
            (
                "watch_deleted",
                CounterType::Gauged,
                CounterValue::Unsigned(self.watch_deleted.swap(0, Ordering::Relaxed) as u64),
            ),
            (
                "watch_restarted",
                CounterType::Gauged,
                CounterValue::Unsigned(self.watch_restarted.swap(0, Ordering::Relaxed) as u64),
            ),
        ]
    }
}

#[derive(Clone)]
pub struct WatcherConfig {
    pub list_limit: u32,
    pub list_interval: Duration,
    pub max_memory: u64,
    pub memory_trim_percent: Option<u8>,
}

// 发生错误，需要重新构造实例
#[derive(Clone)]
pub struct ResourceWatcher<K> {
    api: Api<K>,
    entries: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    err_msg: Arc<Mutex<Option<String>>>,
    kind: Resource,
    version: Arc<AtomicU64>,
    runtime: Handle,
    ready: Arc<AtomicBool>,
    stats_counter: Arc<WatcherCounter>,
    config: WatcherConfig,

    listing: Arc<AtomicBool>,
}

struct Context<K> {
    entries: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    version: Arc<AtomicU64>,
    api: Api<K>,
    kind: Resource,
    err_msg: Arc<Mutex<Option<String>>>,
    ready: Arc<AtomicBool>,
    stats_counter: Arc<WatcherCounter>,
    config: WatcherConfig,
    resource_version: Option<String>,

    listing: Arc<AtomicBool>,
}

impl<K> Watcher for ResourceWatcher<K>
where
    K: Clone + Debug + DeserializeOwned + KubeResource + Serialize + Trimmable,
{
    fn start(&self) -> Option<JoinHandle<()>> {
        let ctx = Context {
            entries: self.entries.clone(),
            version: self.version.clone(),
            kind: self.kind.clone(),
            err_msg: self.err_msg.clone(),
            ready: self.ready.clone(),
            api: self.api.clone(),
            stats_counter: self.stats_counter.clone(),
            config: self.config.clone(),
            resource_version: None,
            listing: self.listing.clone(),
        };

        let handle = self.runtime.spawn(Self::process(ctx));
        info!("{} watcher started", self.kind);
        Some(handle)
    }

    fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    fn error(&self) -> Option<String> {
        self.err_msg.blocking_lock().take()
    }

    fn pb_name(&self) -> &str {
        self.kind.pb_name
    }

    fn entries(&self) -> Vec<Vec<u8>> {
        self.entries
            .blocking_lock()
            .values()
            .map(Clone::clone)
            .collect::<Vec<_>>()
    }

    fn ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

impl<K> ResourceWatcher<K>
where
    K: Clone + Debug + DeserializeOwned + KubeResource + Serialize + Trimmable,
{
    pub fn new(
        api: Api<K>,
        kind: Resource,
        runtime: Handle,
        config: &WatcherConfig,
        listing: Arc<AtomicBool>,
    ) -> Self {
        Self {
            api,
            entries: Arc::new(Mutex::new(HashMap::new())),
            version: Arc::new(AtomicU64::new(0)),
            kind,
            err_msg: Arc::new(Mutex::new(None)),
            runtime,
            ready: Default::default(),
            stats_counter: Default::default(),
            config: config.clone(),
            listing,
        }
    }

    async fn process(mut ctx: Context<K>) {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        Self::serialized_get_list_entry(&mut ctx, &mut encoder).await;
        ctx.ready.store(true, Ordering::Relaxed);
        info!("{} watcher ready", ctx.kind);

        let mut last_update = SystemTime::now();
        let mut stream = runtime::watcher(ctx.api.clone(), ListParams::default()).boxed();

        // If the watch is successful, keep updating the entry with the watch. If the watch is not successful,
        // update the entry with the full amount every 10 minutes.
        loop {
            while let Ok(Some(event)) = stream.try_next().await {
                Self::resolve_event(&ctx, &mut encoder, event).await;
            }
            if last_update.elapsed().unwrap() >= ctx.config.list_interval {
                Self::full_sync(&mut ctx, &mut encoder).await;
                last_update = SystemTime::now();
            }
            time::sleep(SLEEP_INTERVAL).await;
        }
    }

    async fn full_sync(ctx: &mut Context<K>, encoder: &mut ZlibEncoder<Vec<u8>>) {
        let now = Instant::now();
        Self::serialized_get_list_entry(ctx, encoder).await;
        ctx.stats_counter
            .list_cost_time_sum
            .fetch_add(now.elapsed().as_nanos() as u64, Ordering::Relaxed);
        ctx.stats_counter.list_count.fetch_add(1, Ordering::Relaxed);
    }

    async fn serialized_get_list_entry(ctx: &mut Context<K>, encoder: &mut ZlibEncoder<Vec<u8>>) {
        while let Err(_) =
            ctx.listing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            time::sleep(SPIN_INTERVAL).await;
        }
        Self::get_list_entry(ctx, encoder).await;
        ctx.listing.store(false, Ordering::SeqCst);
    }

    fn memory_trim(ctx: &mut Context<K>) {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        if let Some(percent) = ctx.config.memory_trim_percent {
            let process = match procfs::process::Process::myself() {
                Ok(p) => p,
                Err(e) => {
                    warn!("Get self process failed: {}", e);
                    return;
                }
            };
            let status = match process.status() {
                Ok(s) => s,
                Err(e) => {
                    warn!("Get process status failed: {}", e);
                    return;
                }
            };
            let max_memory = ctx.config.max_memory >> 10;
            debug!(
                "max memory: {}KB Rss: anon={}KB file={}KB shmem={}KB",
                max_memory,
                status.rssanon.unwrap_or_default(),
                status.rssfile.unwrap_or_default(),
                status.rssshmem.unwrap_or_default()
            );
            if max_memory
                <= status.rssfile.unwrap_or_default() + status.rssshmem.unwrap_or_default()
            {
                warn!(
                    "max memory {}KB is smaller than RssFile + RssShmem",
                    max_memory
                );
                return;
            }
            let real_percent = status.rssanon.unwrap_or_default() * 100
                / (max_memory
                    - status.rssfile.unwrap_or_default()
                    - status.rssshmem.unwrap_or_default());
            if real_percent < percent as u64 {
                debug!(
                    "RssAnon percent is {} < {}, skip trimming",
                    real_percent, percent
                );
                return;
            }
            unsafe {
                if libc::malloc_trim(0) > 0 {
                    debug!("malloc_trim released memory");
                } else {
                    debug!("malloc_trim cannot release memory");
                }
            }
        }
    }

    // calling list on multiple resources simultaneously may consume a lot of memory
    // use serialized_get_list_entry to avoid oom
    async fn get_list_entry(ctx: &mut Context<K>, encoder: &mut ZlibEncoder<Vec<u8>>) {
        info!(
            "list {} entries with limit {}, memory trim percent: {:?}",
            ctx.kind, ctx.config.list_limit, ctx.config.memory_trim_percent,
        );
        let mut all_entries = HashMap::new();
        let mut total_count = 0;
        let mut total_bytes = 0;
        let mut params = ListParams::default().limit(ctx.config.list_limit);
        loop {
            trace!("{} list with {:?}", ctx.kind, params);
            match ctx.api.list(&params).await {
                Ok(mut object_list) => {
                    total_count += object_list.items.len();
                    if ctx.resource_version.is_some()
                        && ctx.resource_version == object_list.metadata.resource_version
                    {
                        debug!("skip {} list with same resource version", ctx.kind);
                        ctx.stats_counter
                            .list_length
                            .fetch_add(total_count as u32, Ordering::Relaxed);
                        return;
                    }
                    debug!(
                        "{} list returns {} entries, {} remaining",
                        ctx.kind,
                        object_list.items.len(),
                        object_list
                            .metadata
                            .remaining_item_count
                            .unwrap_or_default()
                    );

                    for object in object_list.items {
                        if object.meta().uid.as_ref().is_none() {
                            continue;
                        }
                        let mut trim_object = object.trim();
                        match serde_json::to_vec(&trim_object) {
                            Ok(serialized_object) => {
                                let compressed_object = match Self::compress_entry(
                                    encoder,
                                    serialized_object.as_slice(),
                                ) {
                                    Ok(c) => c,
                                    Err(e) => {
                                        warn!(
                                            "failed to compress {} resource with UID({}) error: {} ",
                                            ctx.kind,
                                            trim_object.meta().uid.as_ref().unwrap(),
                                            e
                                        );
                                        continue;
                                    }
                                };
                                total_bytes += compressed_object.len();
                                all_entries.insert(
                                    trim_object.meta_mut().uid.take().unwrap(),
                                    compressed_object,
                                );
                            }
                            Err(e) => warn!(
                                "failed serialized resource {} UID({}) to json Err: {}",
                                ctx.kind,
                                trim_object.meta().uid.as_ref().unwrap(),
                                e
                            ),
                        }
                    }

                    match object_list.metadata.continue_.as_ref().map(String::as_str) {
                        // sometimes k8s api return Some("") instead of None even if
                        // there is no more entries
                        None | Some("") => {
                            info!(
                                "list {} returned {} entries in {}B",
                                ctx.kind, total_count, total_bytes
                            );
                            if !all_entries.is_empty() {
                                *ctx.entries.lock().await = all_entries;
                                ctx.version.fetch_add(1, Ordering::SeqCst);
                            }
                            ctx.resource_version = object_list.metadata.resource_version.take();
                            ctx.stats_counter
                                .list_length
                                .fetch_add(total_count as u32, Ordering::Relaxed);
                            Self::memory_trim(ctx);
                            return;
                        }
                        _ => (),
                    }
                    params.continue_token = object_list.metadata.continue_.take();

                    Self::memory_trim(ctx);
                }
                Err(err) => {
                    ctx.stats_counter.list_error.fetch_add(1, Ordering::Relaxed);
                    let msg = format!("{} watcher list failed: {}", ctx.kind, err);
                    warn!("{}", msg);
                    ctx.err_msg.lock().await.replace(msg);
                    return;
                }
            }
        }
    }

    async fn resolve_event(ctx: &Context<K>, encoder: &mut ZlibEncoder<Vec<u8>>, event: Event<K>) {
        match event {
            Event::Applied(object) => {
                Self::insert_object(encoder, object, &ctx.entries, &ctx.version, &ctx.kind).await;
                ctx.stats_counter
                    .watch_applied
                    .fetch_add(1, Ordering::Relaxed);
            }
            Event::Deleted(mut object) => {
                if let Some(uid) = object.meta_mut().uid.take() {
                    // 只有删除时检查是否需要更新版本号，其余消息直接更新map内容
                    if ctx.entries.lock().await.remove(&uid).is_some() {
                        ctx.version.fetch_add(1, Ordering::SeqCst);
                    }
                    ctx.stats_counter
                        .watch_deleted
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Event::Restarted(mut objects) => {
                // 按照语义重启后应该拿改key对应最新的state，所以只取restart的最后一个
                // restarted 存储的是某个key对应的object在重启过程中不同状态
                if let Some(object) = objects.pop() {
                    Self::insert_object(encoder, object, &ctx.entries, &ctx.version, &ctx.kind)
                        .await;
                    ctx.stats_counter
                        .watch_restarted
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn insert_object(
        encoder: &mut ZlibEncoder<Vec<u8>>,
        object: K,
        entries: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
        version: &Arc<AtomicU64>,
        kind: &Resource,
    ) {
        let uid = object.meta().uid.clone();
        if let Some(uid) = uid {
            let trim_object = object.trim();
            let serialized_object = serde_json::to_vec(&trim_object);
            match serialized_object {
                Ok(serobj) => {
                    let compressed_object = match Self::compress_entry(encoder, serobj.as_slice()) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(
                                "failed to compress {} resource with UID({}) error: {} ",
                                kind,
                                trim_object.meta().uid.as_ref().unwrap(),
                                e
                            );
                            return;
                        }
                    };
                    entries.lock().await.insert(uid, compressed_object);
                    version.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => debug!(
                    "failed serialized resource {} UID({}) to json Err: {}",
                    kind, uid, e
                ),
            }
        }
    }

    fn compress_entry(encoder: &mut ZlibEncoder<Vec<u8>>, entry: &[u8]) -> io::Result<Vec<u8>> {
        encoder.write_all(entry)?;
        encoder.reset(vec![])
    }
}

pub trait Trimmable: 'static + Send {
    fn trim(self) -> Self;
}

impl Trimmable for Pod {
    fn trim(mut self) -> Self {
        let mut trim_pod = Pod::default();
        trim_pod.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            owner_references: self.metadata.owner_references.take(),
            creation_timestamp: self.metadata.creation_timestamp.take(),
            labels: self.metadata.labels.take(),
            annotations: self.metadata.annotations.take(),
            ..Default::default()
        };
        if let Some(spec) = self.spec.take() {
            trim_pod.spec = Some(PodSpec {
                containers: spec
                    .containers
                    .into_iter()
                    .map(|mut c| Container {
                        name: c.name,
                        env: c.env.take(),
                        ..Default::default()
                    })
                    .collect(),
                node_name: spec.node_name,
                ..Default::default()
            });
        }
        if let Some(pod_status) = self.status.take() {
            trim_pod.status = Some(PodStatus {
                host_ip: pod_status.host_ip,
                conditions: pod_status.conditions,
                container_statuses: pod_status.container_statuses.map(|cs| {
                    cs.into_iter()
                        .map(|mut s| ContainerStatus {
                            container_id: s.container_id.take(),
                            ..Default::default()
                        })
                        .collect()
                }),
                pod_ip: pod_status.pod_ip,
                ..Default::default()
            });
        }
        trim_pod
    }
}

impl Trimmable for Node {
    fn trim(mut self) -> Self {
        let mut trim_node = Node::default();
        trim_node.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            labels: self.metadata.labels.take(),
            ..Default::default()
        };

        if let Some(node_status) = self.status.take() {
            trim_node.status = Some(NodeStatus {
                addresses: node_status.addresses,
                conditions: node_status.conditions,
                capacity: node_status.capacity,
                ..Default::default()
            });
        }
        if let Some(node_spec) = self.spec.take() {
            trim_node.spec = Some(NodeSpec {
                pod_cidr: node_spec.pod_cidr,
                ..Default::default()
            });
        }
        trim_node
    }
}

impl Trimmable for ReplicaSet {
    fn trim(mut self) -> Self {
        let mut trim_rs = ReplicaSet::default();
        trim_rs.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            owner_references: self.metadata.owner_references.take(),
            labels: self.metadata.labels.take(),
            ..Default::default()
        };

        if let Some(rs_spec) = self.spec.take() {
            trim_rs.spec = Some(ReplicaSetSpec {
                replicas: rs_spec.replicas,
                selector: rs_spec.selector,
                ..Default::default()
            });
        }

        trim_rs
    }
}

impl Trimmable for ReplicationController {
    fn trim(mut self) -> Self {
        let mut trim_rc = ReplicationController::default();
        trim_rc.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            ..Default::default()
        };

        if let Some(rc_spec) = self.spec.take() {
            trim_rc.spec = Some(ReplicationControllerSpec {
                replicas: rc_spec.replicas,
                selector: rc_spec.selector,
                template: rc_spec.template,
                ..Default::default()
            });
        }

        trim_rc
    }
}

impl Trimmable for networking::v1::Ingress {
    fn trim(mut self) -> Self {
        self.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            ..Default::default()
        };
        self.status = None;
        self
    }
}

impl Trimmable for networking::v1beta1::Ingress {
    fn trim(mut self) -> Self {
        self.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            ..Default::default()
        };
        self.status = None;
        self
    }
}

impl Trimmable for extensions::v1beta1::Ingress {
    fn trim(mut self) -> Self {
        self.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            ..Default::default()
        };
        self.status = None;
        self
    }
}

impl Trimmable for Route {
    fn trim(mut self) -> Self {
        self.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            ..Default::default()
        };
        self.status = Default::default();
        self
    }
}

impl Trimmable for DaemonSet {
    fn trim(mut self) -> Self {
        let mut trim_ds = DaemonSet::default();
        trim_ds.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            labels: self.metadata.labels.take(),
            ..Default::default()
        };
        if let Some(ds_spec) = self.spec.take() {
            trim_ds.spec = Some(DaemonSetSpec {
                selector: ds_spec.selector,
                template: ds_spec.template,
                ..Default::default()
            })
        }

        trim_ds
    }
}

impl Trimmable for StatefulSet {
    fn trim(mut self) -> Self {
        let mut trim_st = StatefulSet::default();
        trim_st.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            labels: self.metadata.labels.take(),
            ..Default::default()
        };

        if let Some(st_spec) = self.spec.take() {
            trim_st.spec = Some(StatefulSetSpec {
                replicas: st_spec.replicas,
                selector: st_spec.selector,
                template: st_spec.template,
                ..Default::default()
            })
        }
        trim_st
    }
}

impl Trimmable for Deployment {
    fn trim(mut self) -> Self {
        let mut trim_de = Deployment::default();
        trim_de.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            labels: self.metadata.labels.take(),
            ..Default::default()
        };

        if let Some(de_spec) = self.spec.take() {
            trim_de.spec = Some(DeploymentSpec {
                replicas: de_spec.replicas,
                selector: de_spec.selector,
                template: de_spec.template,
                ..Default::default()
            });
        }

        trim_de
    }
}

impl Trimmable for Service {
    fn trim(mut self) -> Self {
        let mut trim_svc = Service::default();
        trim_svc.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            namespace: self.metadata.namespace.take(),
            annotations: self.metadata.annotations.take(),
            labels: self.metadata.labels.take(),
            ..Default::default()
        };

        if let Some(svc_spec) = self.spec.take() {
            trim_svc.spec = Some(ServiceSpec {
                selector: svc_spec.selector,
                type_: svc_spec.type_,
                cluster_ip: svc_spec.cluster_ip,
                ports: svc_spec.ports,
                ..Default::default()
            });
        }
        trim_svc
    }
}

impl Trimmable for Namespace {
    fn trim(mut self) -> Self {
        let mut trim_ns = Namespace::default();
        trim_ns.metadata = ObjectMeta {
            uid: self.metadata.uid.take(),
            name: self.metadata.name.take(),
            ..Default::default()
        };
        trim_ns
    }
}

pub struct ResourceWatcherFactory {
    client: Client,
    runtime: Handle,

    // serialize list operation
    listing: Arc<AtomicBool>,
}

impl ResourceWatcherFactory {
    pub fn new(client: Client, runtime: Handle) -> Self {
        Self {
            client,
            runtime,
            listing: Default::default(),
        }
    }

    fn new_watcher_inner<K>(
        &self,
        kind: Resource,
        stats_collector: &stats::Collector,
        namespace: Option<&str>,
        config: &WatcherConfig,
    ) -> ResourceWatcher<K>
    where
        K: Clone + Debug + DeserializeOwned + KubeResource + Serialize + Trimmable,
        <K as KubeResource>::DynamicType: Default,
    {
        let watcher = ResourceWatcher::new(
            match namespace {
                Some(namespace) => Api::namespaced(self.client.clone(), namespace),
                None => Api::all(self.client.clone()),
            },
            kind,
            self.runtime.clone(),
            config,
            self.listing.clone(),
        );
        stats_collector.register_countable(
            "resource_watcher",
            Countable::Ref(Arc::downgrade(&watcher.stats_counter) as Weak<dyn RefCountable>),
            vec![StatsOption::Tag("kind", watcher.kind.to_string())],
        );
        watcher
    }

    pub fn new_watcher(
        &self,
        resource: Resource,
        namespace: Option<&str>,
        stats_collector: &stats::Collector,
        config: &WatcherConfig,
    ) -> Option<GenericResourceWatcher> {
        let watcher = match resource.name {
            // 特定namespace不支持Node/Namespace资源
            "nodes" => GenericResourceWatcher::Node(self.new_watcher_inner(
                resource,
                stats_collector,
                None,
                config,
            )),
            "namespaces" => GenericResourceWatcher::Namespace(self.new_watcher_inner(
                resource,
                stats_collector,
                None,
                config,
            )),
            "services" => GenericResourceWatcher::Service(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            "deployments" => GenericResourceWatcher::Deployment(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            "pods" => GenericResourceWatcher::Pod(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            "statefulsets" => match resource.selected_gv.as_ref().unwrap() {
                GroupVersion {
                    group: "apps.kruise.io",
                    version: "v1beta1",
                } => GenericResourceWatcher::KruiseStatefulSet(self.new_watcher_inner(
                    resource,
                    stats_collector,
                    namespace,
                    config,
                )),
                GroupVersion {
                    group: "apps",
                    version: "v1",
                } => GenericResourceWatcher::StatefulSet(self.new_watcher_inner(
                    resource,
                    stats_collector,
                    namespace,
                    config,
                )),
                _ => {
                    warn!(
                        "unsupported resource {} group version {}",
                        resource.name,
                        resource.selected_gv.as_ref().unwrap()
                    );
                    return None;
                }
            },
            "daemonsets" => GenericResourceWatcher::DaemonSet(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            "replicationcontrollers" => GenericResourceWatcher::ReplicationController(
                self.new_watcher_inner(resource, stats_collector, namespace, config),
            ),
            "replicasets" => GenericResourceWatcher::ReplicaSet(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            "ingresses" => match resource.selected_gv.as_ref().unwrap() {
                GroupVersion {
                    group: "networking.k8s.io",
                    version: "v1",
                } => GenericResourceWatcher::V1Ingress(self.new_watcher_inner(
                    resource,
                    stats_collector,
                    namespace,
                    config,
                )),
                GroupVersion {
                    group: "networking.k8s.io",
                    version: "v1beta1",
                } => GenericResourceWatcher::V1beta1Ingress(self.new_watcher_inner(
                    resource,
                    stats_collector,
                    namespace,
                    config,
                )),
                GroupVersion {
                    group: "extensions",
                    version: "v1beta1",
                } => GenericResourceWatcher::ExtV1beta1Ingress(self.new_watcher_inner(
                    resource,
                    stats_collector,
                    namespace,
                    config,
                )),
                _ => {
                    warn!(
                        "unsupported resource {} group version {}",
                        resource.name,
                        resource.selected_gv.as_ref().unwrap()
                    );
                    return None;
                }
            },
            "routes" => GenericResourceWatcher::Route(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            "servicerules" => GenericResourceWatcher::ServiceRule(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            "clonesets" => GenericResourceWatcher::CloneSet(self.new_watcher_inner(
                resource,
                stats_collector,
                namespace,
                config,
            )),
            _ => {
                warn!("unsupported resource {}", resource.name);
                return None;
            }
        };

        Some(watcher)
    }
}

```

### sidecar_poller.cs:

```
/*这段代码的作用是创建一个网络轮询器 SidecarPoller，用于获取和管理网络接口信息。
它通过指定的目标 IP 地址获取控制 IP 和 MAC 地址，并根据 MAC 地址找到匹配的接口。
然后，将相关信息存储在 InterfaceInfo 结构体中，并提供了一系列方法来获取和操作接口信息。*/

use std::{net::IpAddr, path::Path, process, thread, time::Duration};

use log::{info, warn};
use regex::Regex;

use super::Poller;
use crate::utils::environment::get_ctrl_ip_and_mac;

use public::{
    netns::{self, InterfaceInfo, NsFile},
    utils::net::link_list,
};

pub struct SidecarPoller(InterfaceInfo);

/*SidecarPoller 结构体的实现块，包含了 new 方法用于创建 SidecarPoller 实例。
在 new 方法中，通过目标 IP 地址获取控制 IP 和 MAC 地址，并获取网络接口列表。
然后根据 MAC 地址找到匹配的接口，打开当前命名空间的网络命名空间文件。
最后，创建一个 InterfaceInfo 结构体实例并返回。*/
impl SidecarPoller {
    pub fn new(dest: IpAddr) -> Self {
        // 通过目标 IP 地址获取控制 IP 和 MAC 地址
        let (ctrl_ip, ctrl_mac) = get_ctrl_ip_and_mac(dest);
        
        // 获取网络接口列表
        let Ok(links) = link_list() else {
            warn!("call link_list() failed");
            thread::sleep(Duration::from_secs(1));
            process::exit(-1);
        };
        
        // 从接口列表中找到与控制 MAC 地址匹配的接口
        let Some(link) = links.into_iter().filter(|link| link.mac_addr == ctrl_mac).next() else {
            warn!("cannot find ctrl interface with mac {}", ctrl_mac);
            thread::sleep(Duration::from_secs(1));
            process::exit(-1);
        };
        
        // 打开当前命名空间的网络命名空间文件
        let Ok(ns): Result<NsFile, _> = Path::new(netns::CURRENT_NS_PATH).try_into() else {
            warn!("cannot open ns file {}", netns::CURRENT_NS_PATH);
            thread::sleep(Duration::from_secs(1));
            process::exit(-1);
        };
        
        // 创建一个 InterfaceInfo 结构体实例
        let info = InterfaceInfo {
            tap_idx: link.if_index,
            mac: ctrl_mac,
            ips: vec![ctrl_ip],
            name: link.name,
            device_id: ns.to_string(),
            tap_ns: ns,
            ..Default::default()
        };
        
        info!("Sidecar poller: {:?}", info);
        
        Self(info)
    }
}

/*Poller trait 的实现块，为 SidecarPoller 结构体实现了各种方法。其中包括获取版本号、获取接口信息、设置网络命名空间正则表达式、启动和停止等方法。*/
impl Poller for SidecarPoller {
    fn get_version(&self) -> u64 {
        1
    }

    fn get_interface_info_in(&self, _: &NsFile) -> Option<Vec<InterfaceInfo>> {
        Some(self.get_interface_info())
    }

    fn get_interface_info(&self) -> Vec<InterfaceInfo> {
        vec![self.0.clone()]
    }

    fn set_netns_regex(&self, _: Option<Regex>) {}

    fn start(&self) {}

    fn stop(&self) {}
}

```

