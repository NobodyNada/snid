//! Simple stress test, designed to be run against the Super Metroid practice hack.

use std::sync::Arc;

use anyhow::Result;
use rand::{Rng, SeedableRng, rngs::SmallRng, seq::IndexedRandom};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::snes::{self, Snes};

struct Memory {
    label: String,
    start: u32,
    len: usize,
    contents: Mutex<Vec<u8>>,
}

impl Memory {
    fn new(label: impl ToString, start: u32, len: usize) -> Arc<Self> {
        let mut contents = Vec::with_capacity(len);
        for i in 0..len {
            contents.push(((start + i as u32) >> 8) as u8);
        }
        Arc::new(Memory {
            label: label.to_string(),
            start,
            len,
            contents: Mutex::new(contents),
        })
    }
}

#[tracing::instrument(name = "stresstest", skip(snes))]
pub async fn run(snes: Arc<Snes>) -> Result<()> {
    // Bank FF doesn't contain anything important
    let rom = Memory::new("ROM", (0xFF_0000 - 0x80_0000) >> 1, 0x8000);
    let wram = Memory::new("WRAM", 0xF6_FC00, 0x400);

    let memories = [rom, wram];
    for memory in &memories {
        loop {
            match snes
                .write(memory.start, memory.contents.lock().await.clone())
                .await
            {
                Err(snes::Error::Disconnected) => continue,
                Ok(()) => break,
                Err(e) => return Err(e.into()),
            }
        }
        info!("Initialized memory {}", memory.label);
    }

    let mut rng = SmallRng::from_os_rng();
    loop {
        let memory = memories.choose(&mut rng).unwrap().clone();
        let offset = rng.random_range(0..memory.len);
        let len = rng.random_range(1..=(memory.len - offset).min(1024));
        if rng.random::<bool>() {
            let data = snes.read(memory.start + offset as u32, len).await?;
            debug!(
                "Read {:#x} bytes from {:#06x}",
                len,
                memory.start + offset as u32
            );
            let expected = &memory.contents.lock().await[offset..][..len];
            let errors = expected
                .iter()
                .zip(data.iter())
                .filter(|(e, r)| e != r)
                .count();
            anyhow::ensure!(
                expected == data,
                "read mismatch:\nexpected {expected:02x?}\n\ngot {data:02x?}\n\nerror rate {}",
                errors as f32 / len as f32
            );
        } else {
            let data: Vec<u8> = (&mut rng).random_iter().take(len).collect();
            snes.write(memory.start + offset as u32, data.clone())
                .await?;
            debug!(
                "Wrote {:#x} bytes to {:#06x}",
                data.len(),
                memory.start + offset as u32
            );
            memory.contents.lock().await[offset..][..len].copy_from_slice(&data);
        };
    }
}
