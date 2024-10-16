use anyhow::{anyhow, bail, Result};
use gridbuffer::error_bail;
use log::{error, info};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::iter;
use std::path::Path;

pub struct LocalFileReader {
    filenames: Vec<String>,
    reader: Option<BufReader<File>>,
    pos: usize,
}

impl LocalFileReader {
    pub fn new(filenames: &Vec<String>) -> Result<Self> {
        // Check if all files exist
        for filename in filenames {
            if !Path::new(filename).exists() {
                error_bail!("File not found: {}", filename.clone());
            }
        }

        Ok(LocalFileReader {
            filenames: filenames.clone(),
            reader: None,
            pos: 0,
        })
    }

    fn open_next_file(&mut self) -> Result<()> {
        if self.pos >= self.filenames.len() {
            error_bail!("no more files");
        }

        let filename = &self.filenames[self.pos];
        let file = File::open(Path::new(filename))?;

        self.pos += 1;
        self.reader = Some(BufReader::new(file));

        Ok(())
    }

    fn read_line_from_next_file(&mut self) -> Option<Result<String>> {
        // Open next file
        match self.open_next_file() {
            Ok(_) => self.next(),
            Err(e) => None,
        }
    }
}

impl Iterator for LocalFileReader {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.as_mut() {
            Some(reader) => match reader.lines().next() {
                Some(line) => Some(line.map_err(|e| e.into())),
                None => self.read_line_from_next_file(),
            },
            None => self.read_line_from_next_file(),
        }
    }
}

/// Get test filenames.
pub fn get_test_feature_filenames(count: usize) -> Vec<String> {
    let filename = "resources/simple_features_nohash_96.txt".to_string();

    (0..count).map(|_| filename.clone()).collect()
}

/// Get test gridbuffer filenames.
pub fn get_test_gridbuffer_filenames(count: usize) -> Vec<String> {
    let filename = "resources/gridbuffers_nohash_row_16_col_81_bitpacking4x.txt".to_string();

    (0..count).map(|_| filename.clone()).collect()
}
