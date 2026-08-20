use anyhow::{bail, Context, Result};
use core_foundation_sys::{
    array::{CFArrayGetCount, CFArrayGetValueAtIndex, CFArrayRef},
    base::{kCFAllocatorDefault, CFGetTypeID, CFRelease, CFTypeRef},
    string::{
        kCFStringEncodingUTF8, CFStringCreateWithCString, CFStringGetCString, CFStringGetLength,
        CFStringGetMaximumSizeForEncoding, CFStringGetTypeID, CFStringRef,
    },
};
use libc::{c_char, c_void, pid_t};
use std::{ffi::CString, process::Command, ptr, thread, time::Duration};

type AXUIElementRef = *const c_void;
type AXError = i32;
type CGEventRef = *const c_void;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct CGPoint {
    x: f64,
    y: f64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct CGSize {
    width: f64,
    height: f64,
}

const AX_ERROR_SUCCESS: AXError = 0;
const AX_ERROR_ATTRIBUTE_UNSUPPORTED: AXError = -25205;
const AX_ERROR_NO_VALUE: AXError = -25212;
const MAX_AX_DEPTH: usize = 64;
const MAX_AX_NODES: usize = 20_000;

#[link(name = "ApplicationServices", kind = "framework")]
extern "C" {
    fn AXUIElementCreateApplication(pid: pid_t) -> AXUIElementRef;
    fn AXUIElementCopyAttributeValue(
        element: AXUIElementRef,
        attribute: CFStringRef,
        value: *mut CFTypeRef,
    ) -> AXError;
    fn AXValueGetValue(value: CFTypeRef, value_type: u32, value: *mut c_void) -> bool;
    fn CGEventCreateMouseEvent(
        source: *const c_void,
        mouse_type: u32,
        mouse_cursor_position: CGPoint,
        mouse_button: u32,
    ) -> CGEventRef;
    fn CGEventSetIntegerValueField(event: CGEventRef, field: u32, value: i64);
    fn CGEventPost(tap: u32, event: CGEventRef);
}

struct OwnedCf(CFTypeRef);

impl Drop for OwnedCf {
    fn drop(&mut self) {
        unsafe { CFRelease(self.0) };
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SimulationAxEvidence {
    pub standard_window_count: usize,
    pub marker_window_count: usize,
    pub unsafe_controls: Vec<String>,
    pub visited_nodes: usize,
}

pub fn verify_reviewed_simulation_window() -> Result<SimulationAxEvidence> {
    review_simulation_window_with_frame().map(|(evidence, _, _)| evidence)
}

fn review_simulation_window_with_frame() -> Result<(SimulationAxEvidence, CGPoint, CGSize)> {
    let pid = unique_process_id("同花顺")?;
    let application = unsafe { AXUIElementCreateApplication(pid) };
    if application.is_null() {
        bail!("failed to open the Tonghuashun accessibility application object");
    }
    let application = OwnedCf(application);
    let windows = copy_attribute(application.0, "AXWindows")?
        .context("Tonghuashun accessibility tree has no windows")?;
    let windows = cf_array_values(&windows)?;
    let mut evidence = SimulationAxEvidence::default();
    let mut simulation_origin = None;
    let mut simulation_size = None;
    for window in windows {
        if copy_string_attribute(window, "AXSubrole")?.as_deref() != Some("AXStandardWindow") {
            continue;
        }
        evidence.standard_window_count += 1;
        let mut window_has_marker = false;
        inspect_element(window, 0, &mut evidence, &mut window_has_marker)?;
        if window_has_marker {
            evidence.marker_window_count += 1;
            simulation_origin = Some(copy_point_attribute(window, "AXPosition")?);
            simulation_size = Some(copy_size_attribute(window, "AXSize")?);
        }
    }
    if evidence.standard_window_count == 0 {
        bail!("Tonghuashun has no stable standard accessibility window");
    }
    if evidence.marker_window_count != 1 {
        bail!(
            "expected one reviewed Tonghuashun simulation window, found {}",
            evidence.marker_window_count
        );
    }
    if !evidence.unsafe_controls.is_empty() {
        bail!(
            "live-account controls are visible in the Tonghuashun simulation window: {}",
            evidence.unsafe_controls.join(",")
        );
    }
    let origin = simulation_origin.context("simulation window has no reviewed position")?;
    let size = simulation_size.context("simulation window has no reviewed size")?;
    if !origin.x.is_finite()
        || !origin.y.is_finite()
        || !size.width.is_finite()
        || !size.height.is_finite()
        || size.width <= 0.0
        || size.height <= 0.0
    {
        bail!("simulation window has an invalid native frame");
    }
    Ok((evidence, origin, size))
}

pub fn verify_reviewed_simulation_point(x: i32, y: i32) -> Result<SimulationAxEvidence> {
    let (evidence, origin, size) = review_simulation_window_with_frame()?;
    let x = f64::from(x);
    let y = f64::from(y);
    if x < origin.x || x > origin.x + size.width || y < origin.y || y > origin.y + size.height {
        bail!("native click point left the current simulation-window frame");
    }
    Ok(evidence)
}

/// Dispatch one native left-button double-click at a point that was proven to
/// belong to the unique reviewed simulation window immediately beforehand.
///
/// This deliberately uses Quartz mouse events. Two accessibility `AXPress`
/// actions are not a double-click in Tonghuashun 5.3.2 and must never be used
/// as a substitute for this operation.
#[allow(non_snake_case)]
pub fn native_double_click(x: i32, y: i32) -> Result<()> {
    verify_reviewed_simulation_point(x, y)
        .context("simulation-window identity or frame changed before native double-click")?;

    let point = CGPoint {
        x: f64::from(x),
        y: f64::from(y),
    };
    let kCGEventLeftMouseDown = 1_u32;
    let kCGEventLeftMouseUp = 2_u32;
    let kCGMouseButtonLeft = 0_u32;
    let kCGMouseEventClickState = 1_u32;
    let kCGHIDEventTap = 0_u32;

    let create = |mouse_type: u32| -> Result<OwnedCf> {
        let event =
            unsafe { CGEventCreateMouseEvent(ptr::null(), mouse_type, point, kCGMouseButtonLeft) };
        if event.is_null() {
            bail!("failed to create native Tonghuashun mouse event");
        }
        Ok(OwnedCf(event))
    };

    let first_down = create(kCGEventLeftMouseDown)?;
    let first_up = create(kCGEventLeftMouseUp)?;
    let second_down = create(kCGEventLeftMouseDown)?;
    let second_up = create(kCGEventLeftMouseUp)?;
    unsafe {
        CGEventSetIntegerValueField(first_down.0, kCGMouseEventClickState, 1);
        CGEventSetIntegerValueField(first_up.0, kCGMouseEventClickState, 1);
        CGEventSetIntegerValueField(second_down.0, kCGMouseEventClickState, 2);
        CGEventSetIntegerValueField(second_up.0, kCGMouseEventClickState, 2);

        CGEventPost(kCGHIDEventTap, first_down.0);
    }
    thread::sleep(Duration::from_millis(12));
    unsafe {
        CGEventPost(kCGHIDEventTap, first_up.0);
    }
    thread::sleep(Duration::from_millis(80));
    unsafe {
        CGEventPost(kCGHIDEventTap, second_down.0);
    }
    thread::sleep(Duration::from_millis(12));
    unsafe {
        CGEventPost(kCGHIDEventTap, second_up.0);
    }
    Ok(())
}

fn inspect_element(
    element: AXUIElementRef,
    depth: usize,
    evidence: &mut SimulationAxEvidence,
    window_has_marker: &mut bool,
) -> Result<()> {
    if depth > MAX_AX_DEPTH {
        bail!("Tonghuashun accessibility tree exceeds the reviewed depth");
    }
    evidence.visited_nodes = evidence
        .visited_nodes
        .checked_add(1)
        .context("Tonghuashun accessibility node count overflow")?;
    if evidence.visited_nodes > MAX_AX_NODES {
        bail!("Tonghuashun accessibility tree exceeds the reviewed node budget");
    }
    let role = copy_string_attribute(element, "AXRole")?
        .context("Tonghuashun accessibility element has no role")?;
    let title = copy_string_attribute(element, "AXTitle")?;
    let value = copy_string_attribute(element, "AXValue")?;
    if role == "AXStaticText" {
        if title.as_deref() == Some("模拟练习") || value.as_deref() == Some("模拟练习") {
            *window_has_marker = true;
        }
        if title.as_deref() == Some("账户设置") || value.as_deref() == Some("账户设置") {
            evidence.unsafe_controls.push("账户设置".to_owned());
        }
    }
    if role == "AXButton" {
        for forbidden in ["转账", "退出"] {
            if title.as_deref() == Some(forbidden) || value.as_deref() == Some(forbidden) {
                evidence.unsafe_controls.push(forbidden.to_owned());
            }
        }
    }
    let Some(children) = copy_attribute(element, "AXChildren")? else {
        return Ok(());
    };
    for child in cf_array_values(&children)? {
        inspect_element(child, depth + 1, evidence, window_has_marker)?;
    }
    Ok(())
}

fn unique_process_id(name: &str) -> Result<pid_t> {
    let output = Command::new("/usr/bin/pgrep")
        .args(["-x", name])
        .output()
        .context("failed to enumerate the Tonghuashun process")?;
    if !output.status.success() {
        bail!("Tonghuashun process is not running");
    }
    let ids = String::from_utf8(output.stdout)
        .context("Tonghuashun process IDs were not UTF-8")?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim()
                .parse::<pid_t>()
                .context("invalid Tonghuashun process ID")
        })
        .collect::<Result<Vec<_>>>()?;
    let [pid] = ids.as_slice() else {
        bail!("expected one Tonghuashun process, found {}", ids.len());
    };
    Ok(*pid)
}

fn copy_attribute(element: AXUIElementRef, name: &str) -> Result<Option<OwnedCf>> {
    let name = CString::new(name).context("invalid accessibility attribute name")?;
    let attribute = unsafe {
        CFStringCreateWithCString(kCFAllocatorDefault, name.as_ptr(), kCFStringEncodingUTF8)
    };
    if attribute.is_null() {
        bail!("failed to allocate accessibility attribute name");
    }
    let _attribute_guard = OwnedCf(attribute.cast());
    let mut value: CFTypeRef = ptr::null();
    let error = unsafe { AXUIElementCopyAttributeValue(element, attribute, &mut value) };
    match error {
        AX_ERROR_SUCCESS => {
            if value.is_null() {
                bail!("accessibility attribute succeeded without a value");
            }
            Ok(Some(OwnedCf(value)))
        }
        AX_ERROR_ATTRIBUTE_UNSUPPORTED | AX_ERROR_NO_VALUE => Ok(None),
        other => bail!("accessibility attribute read failed with AXError {other}"),
    }
}

fn copy_point_attribute(element: AXUIElementRef, name: &str) -> Result<CGPoint> {
    let value = copy_attribute(element, name)?
        .with_context(|| format!("accessibility element has no {name}"))?;
    let mut point = CGPoint { x: 0.0, y: 0.0 };
    let copied =
        unsafe { AXValueGetValue(value.0, 1, (&mut point as *mut CGPoint).cast::<c_void>()) };
    if !copied {
        bail!("accessibility {name} is not a CGPoint");
    }
    Ok(point)
}

fn copy_size_attribute(element: AXUIElementRef, name: &str) -> Result<CGSize> {
    let value = copy_attribute(element, name)?
        .with_context(|| format!("accessibility element has no {name}"))?;
    let mut size = CGSize {
        width: 0.0,
        height: 0.0,
    };
    let copied =
        unsafe { AXValueGetValue(value.0, 2, (&mut size as *mut CGSize).cast::<c_void>()) };
    if !copied {
        bail!("accessibility {name} is not a CGSize");
    }
    Ok(size)
}

fn copy_string_attribute(element: AXUIElementRef, name: &str) -> Result<Option<String>> {
    let Some(value) = copy_attribute(element, name)? else {
        return Ok(None);
    };
    if unsafe { CFGetTypeID(value.0) } != unsafe { CFStringGetTypeID() } {
        return Ok(None);
    }
    let string = value.0 as CFStringRef;
    let length = unsafe { CFStringGetLength(string) };
    let maximum = unsafe { CFStringGetMaximumSizeForEncoding(length, kCFStringEncodingUTF8) };
    let capacity = usize::try_from(maximum)
        .context("accessibility string is too large")?
        .checked_add(1)
        .context("accessibility string size overflow")?;
    let mut buffer = vec![0_u8; capacity];
    let copied = unsafe {
        CFStringGetCString(
            string,
            buffer.as_mut_ptr().cast::<c_char>(),
            capacity as isize,
            kCFStringEncodingUTF8,
        )
    };
    if copied == 0 {
        bail!("failed to decode accessibility string");
    }
    let end = buffer
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(buffer.len());
    String::from_utf8(buffer[..end].to_vec())
        .context("accessibility string was not valid UTF-8")
        .map(Some)
}

fn cf_array_values(array: &OwnedCf) -> Result<Vec<AXUIElementRef>> {
    let array = array.0 as CFArrayRef;
    let count = unsafe { CFArrayGetCount(array) };
    let count = usize::try_from(count).context("negative accessibility array length")?;
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let value = unsafe { CFArrayGetValueAtIndex(array, index as isize) };
        if value.is_null() {
            bail!("accessibility array contains a null element");
        }
        values.push(value);
    }
    Ok(values)
}
