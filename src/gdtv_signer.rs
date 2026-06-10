use anyhow::{Context, Result};
use std::{
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};
use wasmi::{Caller, Engine, Extern, Linker, Memory, Module, Store, TypedFunc};

const WASM: &[u8] = include_bytes!("gdtv_webqs.wasm");
const OFFICIAL_LOCATION_HREF: &str = "https://www.gdtv.cn/";
const OFFICIAL_LOCATION_HOST: &str = "www.gdtv.cn";
const OFFICIAL_LOCATION_ORIGIN: &str = "https://www.gdtv.cn";
static SIGNER_MODULE: OnceLock<Result<SignerModule, String>> = OnceLock::new();

type SignFunc = TypedFunc<(i32, i32, i32, i32, i32, i32, i32, i32, i32, i32, i32), i32>;

struct SignerModule {
    engine: Engine,
    module: Module,
}

fn signer_module() -> Result<&'static SignerModule> {
    SIGNER_MODULE
        .get_or_init(|| {
            let engine = Engine::default();
            let module = Module::new(&engine, WASM).map_err(|error| error.to_string())?;
            Ok(SignerModule { engine, module })
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))
}

#[derive(Clone, Debug)]
enum JsVal {
    Undefined,
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Object(&'static str),
    Location,
    Function(String),
    Map(Vec<(String, String)>),
    Date(f64),
}

#[derive(Debug)]
pub struct SignedHeaders {
    pub timestamp: String,
    pub signature: String,
    pub key: String,
    pub client: String,
    pub device_id: String,
}

struct HostState {
    heap: Vec<JsVal>,
    free: usize,
    now_ms: i64,
}

impl HostState {
    fn new(now_ms: i64) -> Self {
        let mut heap = vec![JsVal::Undefined; 128];
        heap.push(JsVal::Undefined);
        heap.push(JsVal::Null);
        heap.push(JsVal::Bool(true));
        heap.push(JsVal::Bool(false));
        Self {
            heap,
            free: 132,
            now_ms,
        }
    }

    fn get(&self, idx: i32) -> JsVal {
        self.heap
            .get(idx as usize)
            .cloned()
            .unwrap_or(JsVal::Undefined)
    }

    fn add(&mut self, value: JsVal) -> i32 {
        let idx = self.free;
        if idx == self.heap.len() {
            self.heap.push(JsVal::Number((idx + 1) as f64));
        }
        self.free = match self.heap[idx] {
            JsVal::Number(next) => next as usize,
            _ => self.heap.len(),
        };
        self.heap[idx] = value;
        idx as i32
    }

    fn drop_ref(&mut self, idx: i32) {
        let idx = idx as usize;
        if idx >= 132 && idx < self.heap.len() {
            self.heap[idx] = JsVal::Number(self.free as f64);
            self.free = idx;
        }
    }
}

pub fn sign_get(url: &str, device_id: &str, client: &str) -> Result<SignedHeaders> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()
        .context("system timestamp does not fit i64")?;
    sign_impl("GET", url, device_id, client, "", now_ms)
}

#[cfg(test)]
fn sign_get_at(url: &str, device_id: &str, client: &str, now_ms: i64) -> Result<SignedHeaders> {
    sign_impl("GET", url, device_id, client, "", now_ms)
}

fn sign_impl(
    method: &str,
    url: &str,
    device_id: &str,
    client_name: &str,
    body: &str,
    now_ms: i64,
) -> Result<SignedHeaders> {
    let signer = signer_module()?;
    let mut store = Store::new(&signer.engine, HostState::new(now_ms));
    let mut linker = Linker::new(&signer.engine);
    define_host_imports(&mut linker)?;
    let instance = linker.instantiate_and_start(&mut store, &signer.module)?;

    let malloc: TypedFunc<(i32, i32), i32> =
        instance.get_typed_func(&store, "__wbindgen_export_0")?;
    let sign: SignFunc = instance.get_typed_func(&store, "a")?;
    let memory = instance
        .get_memory(&store, "memory")
        .context("WASM memory export missing")?;

    let (method_ptr, method_len) = write_string(&mut store, memory, malloc, method)?;
    let (url_ptr, url_len) = write_string(&mut store, memory, malloc, url)?;
    let (device_ptr, device_len) = write_string(&mut store, memory, malloc, device_id)?;
    let (client_ptr, client_len) = write_string(&mut store, memory, malloc, client_name)?;
    let (body_ptr, body_len) = write_string(&mut store, memory, malloc, body)?;

    // Mirrors wasm-bindgen's temporary stack slot for an undefined optional scope.
    store.data_mut().heap[127] = JsVal::Undefined;
    let result_idx = sign.call(
        &mut store,
        (
            method_ptr, method_len, url_ptr, url_len, device_ptr, device_len, client_ptr,
            client_len, body_ptr, body_len, 127,
        ),
    )?;

    headers_from_js_map(store.data().get(result_idx))
}

fn write_string(
    store: &mut Store<HostState>,
    memory: Memory,
    malloc: TypedFunc<(i32, i32), i32>,
    value: &str,
) -> Result<(i32, i32)> {
    let ptr = malloc.call(&mut *store, (value.len() as i32, 1))?;
    memory.write(&mut *store, ptr as usize, value.as_bytes())?;
    Ok((ptr, value.len() as i32))
}

fn headers_from_js_map(value: JsVal) -> Result<SignedHeaders> {
    let JsVal::Map(items) = value else {
        anyhow::bail!("GDTV signer returned non-map value")
    };
    let find = |name: &str| {
        items
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
            .with_context(|| format!("GDTV signer omitted {name}"))
    };
    Ok(SignedHeaders {
        timestamp: find("X-ITOUCHTV-Ca-Timestamp")?,
        signature: find("X-ITOUCHTV-Ca-Signature")?,
        key: find("X-ITOUCHTV-Ca-Key")?,
        client: find("X-ITOUCHTV-CLIENT")?,
        device_id: find("X-ITOUCHTV-DEVICE-ID")?,
    })
}

fn memory(caller: &Caller<'_, HostState>) -> anyhow::Result<Memory> {
    match caller.get_export("memory") {
        Some(Extern::Memory(memory)) => Ok(memory),
        Some(_) => anyhow::bail!("memory export has wrong type"),
        None => anyhow::bail!("memory export missing"),
    }
}

fn read_string(caller: &Caller<'_, HostState>, ptr: i32, len: i32) -> String {
    try_read_string(caller, ptr, len).unwrap_or_default()
}

fn try_read_string(caller: &Caller<'_, HostState>, ptr: i32, len: i32) -> anyhow::Result<String> {
    let len = usize::try_from(len).context("negative WASM string length")?;
    let ptr = usize::try_from(ptr).context("negative WASM string pointer")?;
    let memory = memory(caller)?;
    let mut bytes = vec![0_u8; len];
    memory.read(caller, ptr, &mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn write_i32_pair(mut caller: Caller<'_, HostState>, out: i32, ptr: i32, len: i32) {
    let _ = try_write_i32_pair(&mut caller, out, ptr, len);
}

fn try_write_i32_pair(
    caller: &mut Caller<'_, HostState>,
    out: i32,
    ptr: i32,
    len: i32,
) -> anyhow::Result<()> {
    let out = usize::try_from(out).context("negative WASM output pointer")?;
    let memory = memory(caller)?;
    let mut bytes = [0_u8; 8];
    bytes[..4].copy_from_slice(&ptr.to_le_bytes());
    bytes[4..].copy_from_slice(&len.to_le_bytes());
    memory.write(caller, out, &bytes)?;
    Ok(())
}

fn alloc_string(caller: &mut Caller<'_, HostState>, value: &str) -> (i32, i32) {
    try_alloc_string(caller, value).unwrap_or((0, 0))
}

fn try_alloc_string(caller: &mut Caller<'_, HostState>, value: &str) -> anyhow::Result<(i32, i32)> {
    let malloc: TypedFunc<(i32, i32), i32> = caller
        .get_export("__wbindgen_export_0")
        .context("malloc export missing")?
        .into_func()
        .context("malloc export has wrong type")?
        .typed(&caller)?;
    let len = i32::try_from(value.len()).context("WASM string too large")?;
    let ptr = malloc.call(&mut *caller, (len, 1))?;
    let ptr_usize = usize::try_from(ptr).context("negative WASM malloc pointer")?;
    let memory = memory(caller)?;
    memory.write(&mut *caller, ptr_usize, value.as_bytes())?;
    Ok((ptr, len))
}

fn write_string_result(mut caller: Caller<'_, HostState>, out: i32, value: &str) {
    if let Ok((ptr, len)) = try_alloc_string(&mut caller, value) {
        let _ = try_write_i32_pair(&mut caller, out, ptr, len);
    }
}

fn add(caller: &mut Caller<'_, HostState>, value: JsVal) -> i32 {
    caller.data_mut().add(value)
}

fn call_dynamic_function(function: JsVal, this: JsVal, arg: Option<JsVal>) -> JsVal {
    let JsVal::Function(source) = function else {
        return arg.unwrap_or(JsVal::Undefined);
    };
    match (&this, arg.as_ref()) {
        (JsVal::Object(_), Some(JsVal::String(key)))
            if source.contains("this[arguments[0x0]]") || source.contains("this[_$]") =>
        {
            if key == "location" {
                return JsVal::Location;
            }
            return JsVal::Bool(true);
        }
        (JsVal::Location, Some(JsVal::String(key)))
            if source.contains("this[arguments[0x0]]") || source.contains("this[_$]") =>
        {
            return match key.as_str() {
                "href" => JsVal::String(OFFICIAL_LOCATION_HREF.to_owned()),
                "host" => JsVal::String(OFFICIAL_LOCATION_HOST.to_owned()),
                "origin" => JsVal::String(OFFICIAL_LOCATION_ORIGIN.to_owned()),
                _ => JsVal::Bool(true),
            };
        }
        _ => {}
    }
    if source.contains("gdtv2") {
        return JsVal::Number(2048.0);
    }
    if source.contains("window[") && source.contains("instanceof Location") {
        return JsVal::Bool(true);
    }
    if source.contains("documentElement") {
        return JsVal::Bool(false);
    }
    if source.contains("gdtvh") && source.contains("fill") {
        return JsVal::Bool(true);
    }
    if source.contains("gdtvo") {
        return JsVal::Bool(false);
    }
    JsVal::Bool(false)
}

fn define_host_imports(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "wbg",
        "__wbindgen_object_drop_ref",
        |mut caller: Caller<'_, HostState>, idx: i32| {
            caller.data_mut().drop_ref(idx);
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_self_1ff1d729e9aae938",
        |mut caller: Caller<'_, HostState>| -> i32 { add(&mut caller, JsVal::Object("window")) },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_window_5f4faef6c12b79ec",
        |mut caller: Caller<'_, HostState>| -> i32 { add(&mut caller, JsVal::Object("window")) },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_globalThis_1d39714405582d3c",
        |mut caller: Caller<'_, HostState>| -> i32 { add(&mut caller, JsVal::Object("window")) },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_global_651f05c6a0944d1c",
        |mut caller: Caller<'_, HostState>| -> i32 { add(&mut caller, JsVal::Undefined) },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_is_undefined",
        |caller: Caller<'_, HostState>, idx: i32| -> i32 {
            i32::from(matches!(caller.data().get(idx), JsVal::Undefined))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_newnoargs_581967eacc0e2604",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            let source = read_string(&caller, ptr, len);
            add(&mut caller, JsVal::Function(source))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_call_cb65541d95d71282",
        |mut caller: Caller<'_, HostState>, function: i32, this: i32| -> i32 {
            let function = caller.data().get(function);
            let this = caller.data().get(this);
            let value = call_dynamic_function(function, this, None);
            add(&mut caller, value)
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_object_clone_ref",
        |mut caller: Caller<'_, HostState>, idx: i32| -> i32 {
            let value = caller.data().get(idx);
            add(&mut caller, value)
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_instanceof_Window_9029196b662bc42a",
        |caller: Caller<'_, HostState>, idx: i32| -> i32 {
            i32::from(matches!(caller.data().get(idx), JsVal::Object("window")))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_document_f7ace2b956f30a4f",
        |mut caller: Caller<'_, HostState>, _idx: i32| -> i32 {
            add(&mut caller, JsVal::Object("document"))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_location_56243dba507f472d",
        |mut caller: Caller<'_, HostState>, _idx: i32| -> i32 { add(&mut caller, JsVal::Location) },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_host_15090f3de0544fea",
        |caller: Caller<'_, HostState>, out: i32, _idx: i32| {
            write_string_result(caller, out, OFFICIAL_LOCATION_HOST);
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_origin_50aa482fa6784a0a",
        |caller: Caller<'_, HostState>, out: i32, _idx: i32| {
            write_string_result(caller, out, OFFICIAL_LOCATION_ORIGIN);
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_href_d62a28e4fc1ab948",
        |caller: Caller<'_, HostState>, out: i32, _idx: i32| {
            write_string_result(caller, out, OFFICIAL_LOCATION_HREF);
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_newwithargs_a0432b7780c1dfa1",
        |mut caller: Caller<'_, HostState>, p1: i32, l1: i32, p2: i32, l2: i32| -> i32 {
            let source = format!(
                "{}\n{}",
                read_string(&caller, p1, l1),
                read_string(&caller, p2, l2)
            );
            add(&mut caller, JsVal::Function(source))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_string_new",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            let value = read_string(&caller, ptr, len);
            add(&mut caller, JsVal::String(value))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_call_01734de55d61e11d",
        |mut caller: Caller<'_, HostState>, function: i32, this: i32, arg: i32| -> i32 {
            let function = caller.data().get(function);
            let this = caller.data().get(this);
            let arg = caller.data().get(arg);
            let value = call_dynamic_function(function, this, Some(arg));
            add(&mut caller, value)
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_string_get",
        |mut caller: Caller<'_, HostState>, out: i32, idx: i32| {
            if let JsVal::String(value) = caller.data().get(idx) {
                let (ptr, len) = alloc_string(&mut caller, &value);
                write_i32_pair(caller, out, ptr, len);
            } else {
                write_i32_pair(caller, out, 0, 0);
            }
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_eval_8c72ad5eafe427f2",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            let source = read_string(&caller, ptr, len);
            let value = if source.contains("gdtv2") {
                JsVal::Number(2048.0)
            } else {
                JsVal::Bool(false)
            };
            add(&mut caller, value)
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_typeof",
        |mut caller: Caller<'_, HostState>, idx: i32| -> i32 {
            let value = caller.data().get(idx);
            let ty = match value {
                JsVal::Undefined => "undefined",
                JsVal::Bool(_) => "boolean",
                JsVal::Number(_) => "number",
                JsVal::String(_) => "string",
                JsVal::Function(_) => "function",
                _ => "object",
            };
            add(&mut caller, JsVal::String(ty.to_owned()))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_boolean_get",
        |caller: Caller<'_, HostState>, idx: i32| -> i32 {
            match caller.data().get(idx) {
                JsVal::Bool(true) => 1,
                JsVal::Bool(false) => 0,
                _ => 2,
            }
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_new_56693dbed0c32988",
        |mut caller: Caller<'_, HostState>| -> i32 { add(&mut caller, JsVal::Map(Vec::new())) },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_set_bedc3d02d0f05eb0",
        |mut caller: Caller<'_, HostState>, map: i32, key: i32, value: i32| -> i32 {
            let key = match caller.data().get(key) {
                JsVal::String(value) => value,
                other => format!("{other:?}"),
            };
            let value = match caller.data().get(value) {
                JsVal::String(value) => value,
                JsVal::Number(value) => format!("{}", value as i64),
                JsVal::Bool(value) => value.to_string(),
                other => format!("{other:?}"),
            };
            let mut cloned = Vec::new();
            if let Some(JsVal::Map(items)) = caller.data_mut().heap.get_mut(map as usize) {
                items.push((key, value));
                cloned = items.clone();
            }
            add(&mut caller, JsVal::Map(cloned))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_number_new",
        |mut caller: Caller<'_, HostState>, value: f64| -> i32 {
            add(&mut caller, JsVal::Number(value))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_new0_c0be7df4b6bd481f",
        |mut caller: Caller<'_, HostState>| -> i32 {
            let now_ms = caller.data().now_ms;
            add(&mut caller, JsVal::Date(now_ms as f64))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_getTime_5e2054f832d82ec9",
        |caller: Caller<'_, HostState>, idx: i32| -> f64 {
            match caller.data().get(idx) {
                JsVal::Date(value) | JsVal::Number(value) => value,
                _ => caller.data().now_ms as f64,
            }
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_new_cd59bfc8881f487b",
        |mut caller: Caller<'_, HostState>, idx: i32| -> i32 {
            let value = match caller.data().get(idx) {
                JsVal::Number(value) => value,
                _ => 0.0,
            };
            add(&mut caller, JsVal::Date(value))
        },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbg_getTimezoneOffset_8aee3445f323973e",
        |_caller: Caller<'_, HostState>, _idx: i32| -> f64 { 0.0 },
    )?;
    linker.func_wrap(
        "wbg",
        "__wbindgen_throw",
        |caller: Caller<'_, HostState>, ptr: i32, len: i32| -> () {
            let _message = read_string(&caller, ptr, len);
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "wasmi debug interpreter overflows stack; release signer test covers runtime path"
    )]
    fn signer_returns_official_headers() {
        let headers = sign_get(
            "https://gdtv-api.gdtv.cn/api/tv/v2/tvChannel?category=0",
            "WEB_test",
            "WEB_PC",
        )
        .expect("signer works");
        assert_eq!(headers.key, "89541943007407288657755311868534");
        assert_eq!(headers.client, "WEB_PC");
        assert_eq!(headers.device_id, "WEB_test");
        assert!(!headers.timestamp.is_empty());
        assert!(!headers.signature.is_empty());
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "wasmi debug interpreter overflows stack; release signer test covers runtime path"
    )]
    fn signer_matches_captured_detail_request() {
        let headers = sign_get_at(
            "https://gdtv-api.gdtv.cn/api/tv/v2/tvChannel/43?tvChannelPk=43&node=Y2JlNmQ3MGM2ZDdiYjExZDdmNzIxZTA1ZDJhODBiOTAtTGJ3aU1IMHphbmhVWk1OMGQ0R1JQZUhsRDFYMkF3TTZOalc2UGpvQjgzVERsVUVQeERUYSUyRjFyWjdJVGhIOEx3YlVPUm5LS1VqbkpNTURoNTBURFJ6V09uUVpsZW52blZsOTRDeE5rUkclMkZkQ0FtOUNJcG5lWUR4Y2pYZzVONU1zWTVGNzdtQlBCanQ0Mko1cEtNY3IyQ0xMd2JaNElzcmFxRkxRYkdyRTQlMkZJVmhDbk9WZUdWNSUyRm1nWk1nYWtZUkk=",
            "WEB_55aa13c0-5791-11f1-8adb-359518524234",
            "WEB_PC",
            1779641799382,
        )
        .expect("signer works");
        assert_eq!(headers.timestamp, "1779641799382");
        assert_eq!(headers.key, "89541943007407288657755311868534");
        assert_eq!(
            headers.signature,
            "yl4orH6A+cjjoONrUTSAaJG2ZkU58NaI0j1CxiNSNnw="
        );
    }
}
