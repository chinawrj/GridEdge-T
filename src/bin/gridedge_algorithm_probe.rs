use anyhow::{bail, Result};
use gridedge_t::{
    decision::{
        AlgorithmManifest, DecisionRequest, DecisionResponse, RightDecision,
        DECISION_CONTRACT_VERSION,
    },
    gate::{context_hash, GateDecision},
};
use rust_decimal::Decimal;
use std::io::{Read, Write};

fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments
        .iter()
        .any(|argument| argument == "--setsid-descendant")
    {
        #[cfg(unix)]
        unsafe {
            libc::setsid();
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        if let Some(path) = arguments
            .iter()
            .find_map(|argument| argument.strip_prefix("--sentinel="))
        {
            std::fs::write(path, b"setsid-descendant-survived")?;
        }
        std::thread::sleep(std::time::Duration::from_secs(60));
        return Ok(());
    }
    if arguments
        .iter()
        .any(|argument| argument == "--hold-descendant")
    {
        std::thread::sleep(std::time::Duration::from_millis(300));
        if let Some(path) = arguments
            .iter()
            .find_map(|argument| argument.strip_prefix("--sentinel="))
        {
            std::fs::write(path, b"descendant-survived")?;
        }
        std::thread::sleep(std::time::Duration::from_secs(60));
        return Ok(());
    }
    let operation = arguments
        .iter()
        .find(|argument| argument.starts_with("--gridedge-"));
    match operation.map(String::as_str) {
        Some("--gridedge-manifest-v1") => write_json(&AlgorithmManifest {
            algorithm_name: "rust-reference-process-algorithm".to_owned(),
            algorithm_version: "1".to_owned(),
            supported_contract_versions: vec![DECISION_CONTRACT_VERSION],
            deterministic: true,
            supports_checkpoint: arguments
                .iter()
                .any(|argument| argument == "--claim-checkpoint"),
            artifact_sha256: String::new(),
            canonical_arguments: Vec::new(),
            environment_sha256: String::new(),
            platform_sha256: String::new(),
        }),
        Some("--gridedge-decide-v1") => {
            if arguments
                .iter()
                .any(|argument| argument == "--ignore-stdin")
            {
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            if arguments.iter().any(|argument| argument == "--busy") {
                loop {
                    std::hint::spin_loop();
                }
            }
            #[cfg(target_os = "linux")]
            if arguments.iter().any(|argument| argument == "--over-memory") {
                let mut memory = vec![0_u8; 700 * 1024 * 1024];
                for offset in (0..memory.len()).step_by(4096) {
                    memory[offset] = 1;
                }
                std::hint::black_box(memory);
            }
            if arguments.iter().any(|argument| argument == "--hang") {
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            if let Some(sentinel) = arguments
                .iter()
                .find_map(|argument| argument.strip_prefix("--spawn-descendant="))
            {
                let executable = std::env::current_exe()?;
                std::process::Command::new(executable)
                    .arg("--hold-descendant")
                    .arg(format!("--sentinel={sentinel}"))
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::inherit())
                    .stderr(std::process::Stdio::inherit())
                    .spawn()?;
            }
            if let Some(sentinel) = arguments
                .iter()
                .find_map(|argument| argument.strip_prefix("--escape-session="))
            {
                let executable = std::env::current_exe()?;
                std::process::Command::new(executable)
                    .arg("--setsid-descendant")
                    .arg(format!("--sentinel={sentinel}"))
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::inherit())
                    .stderr(std::process::Stdio::inherit())
                    .spawn()?;
            }
            if arguments.iter().any(|argument| argument == "--flood") {
                std::io::stdout().write_all(&vec![b'x'; 2 * 1024 * 1024])?;
                return Ok(());
            }
            let mut input = Vec::new();
            std::io::stdin().read_to_end(&mut input)?;
            let request: DecisionRequest = serde_json::from_slice(&input)?;
            request.validate()?;
            let decision = GateDecision {
                probability: Decimal::ONE,
                alpha: Decimal::ONE,
                action: "EXECUTE".to_owned(),
                reason_codes: vec!["REFERENCE_PROCESS".to_owned()],
                model_name: "rust-reference-process-algorithm".to_owned(),
                model_version: "1".to_owned(),
                input_snapshot_hash: context_hash(&request.context)?,
                decided_at: request.context.current_time,
            };
            let exercise_quantity = request.context.available_quantity;
            write_json(&DecisionResponse {
                contract_version: DECISION_CONTRACT_VERSION,
                request_id: request.request_id,
                right_id: request.right.right_id,
                outcome: RightDecision::Exercise {
                    exercise_quantity,
                    defer_quantity: 0,
                },
                decision,
            })
        }
        _ => bail!("unsupported GridEdge algorithm protocol command"),
    }
}

fn write_json(value: &impl serde::Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    std::io::stdout().write_all(&bytes)?;
    Ok(())
}
