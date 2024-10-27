use anyhow::{bail, Result};

use std::{fs::File, io::BufRead, io::BufReader, iter::Iterator, path::Path};

use droplet_core::{
    error_bail,
    grid_sample::{GridRow, SampleKey},
};
use gridbuffer::core::gridbuffer::GridBuffer;
use log::{error, info};

pub struct LocalGridbufferReader {
    /// Paths to local gridbuffer files.
    paths: Vec<String>,

    /// Key ids.
    ///
    /// Need to check whether key_ids exists in table column ids.
    key_ids: Vec<u32>,

    /// Index of the current path.
    cur_path_index: usize,

    /// File handle of the current file.
    file_reader: BufReader<File>,

    /// Current gridbuffer.
    cur_gridbuffer: Option<GridBuffer>,

    /// Current row index.
    cur_row_index: usize,
}

impl LocalGridbufferReader {
    pub fn new(paths: Vec<String>, key_ids: Vec<u32>) -> Result<Self> {
        if paths.is_empty() {
            error_bail!("No gridbuffer files provided");
        }

        for p in paths.iter() {
            if !Path::new(p).exists() {
                error_bail!("Gridbuffer file {} does not exist", p);
            }
        }

        let file = File::open(paths[0].clone())?;

        Ok(Self {
            paths,
            key_ids,
            cur_path_index: 0,
            file_reader: BufReader::new(file),
            cur_gridbuffer: None,
            cur_row_index: 0,
        })
    }

    fn open_next_file(&mut self) -> Result<()> {
        self.cur_path_index += 1;

        if self.cur_path_index >= self.paths.len() {
            return Err(anyhow::anyhow!("No more gridbuffer files"));
        }

        let file = File::open(self.paths[self.cur_path_index].clone())?;
        self.file_reader = BufReader::new(file);
        Ok(())
    }

    fn read_line(&mut self) -> Result<Option<String>> {
        let mut line = String::new();
        match self.file_reader.read_line(&mut line) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    return Ok(None);
                } else {
                    return Ok(Some(line));
                }
            }
            Err(e) => {
                error_bail!(
                    "Failed to read line from gridbuffer file, filename: {}, error: {}",
                    self.paths[self.cur_path_index],
                    e
                );
            }
        }
    }

    fn read_gridbuffer(&mut self) -> Result<()> {
        match self.read_line() {
            Ok(line_opt) => match line_opt {
                Some(line) => match GridBuffer::from_bytes(line.as_bytes()) {
                    Ok(gridbuffer) => {
                        self.cur_gridbuffer = Some(gridbuffer);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to parse gridbuffer, error: {}", e);
                        return Err(e);
                    }
                },
                None => {
                    self.open_next_file()?;
                    self.read_gridbuffer()
                }
            },
            Err(e) => {
                error!(
                    "Failed to read line from gridbuffer file, open next file, error: {}",
                    e
                );
                self.open_next_file()?;
                self.read_gridbuffer()
            }
        }
    }
}

impl Iterator for LocalGridbufferReader {
    type Item = GridRowRefs;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_line() {
            Ok(line_opt) => match line_opt {
                Some(line) => match GridBuffer::from_bytes(line.as_bytes()) {
                    Ok(gridbuffer) => {
                        let mut rows = Vec::with_capacity(gridbuffer.num_rows());

                        self.cur_gridbuffer = Some(gridbuffer);

                        for i in 0..self.cur_gridbuffer.as_ref().unwrap().num_rows() {
                            let mut row = Vec::with_capacity(self.key_ids.len());

                            for key_id in self.key_ids.iter() {
                                match self.cur_gridbuffer.as_ref().unwrap().get_col_by_id(*key_id) {
                                    Some(col) => {
                                        let cell = GridCellRef {
                                            gridbuffer: self.cur_gridbuffer.as_ref().unwrap(),
                                            row_index: i,
                                            col_index: col,
                                        };

                                        row.push(cell);
                                    }
                                    None => {
                                        error!("column id not found: {}", key_id);
                                        return None;
                                    }
                                }
                            }

                            rows.push(GridRowRef::new(row));
                        }

                        Some(GridRowRefs { rows })
                    }
                    Err(e) => {
                        error!("Failed to parse gridbuffer, error: {}", e);
                        None
                    }
                },
                None => match self.open_next_file() {
                    Ok(_) => self.next(),
                    Err(e) => {
                        error!("Failed to open next gridbuffer file, error: {}", e);
                        None
                    }
                },
            },
            Err(e) => {
                error!(
                    "Failed to read line from gridbuffer file, try next file, error: {}",
                    e
                );

                match self.open_next_file() {
                    Ok(_) => self.next(),
                    Err(e) => {
                        error!("Failed to open next gridbuffer file, error: {}", e);
                        None
                    }
                }
            }
        }
    }
}

pub struct LocalGridRowReader(LocalGridbufferReader);

impl LocalGridRowReader {
    pub fn new(file_paths: Vec<String>, key_ids: Vec<u32>) -> Result<Self> {
        let reader = LocalGridbufferReader::new(file_paths, key_ids)?;
        Ok(Self(reader))
    }
}

impl Iterator for LocalGridRowReader {
    type Item = GridRowRef;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.cur_gridbuffer.as_ref() {
            Some(gridbuffer) => {
                if self.0.cur_row_index < gridbuffer.num_rows() {
                    let mut cells = Vec::with_capacity(self.0.key_ids.len());

                    for key_id in self.0.key_ids.iter() {
                        match gridbuffer.get_col_by_id(*key_id) {
                            Some(col) => {
                                let cell = GridCellRef {
                                    gridbuffer: gridbuffer,
                                    row_index: self.0.cur_row_index,
                                    col_index: col,
                                };

                                cells.push(cell);
                            }
                            None => {
                                error!("column id not found: {}", key_id);
                                return None;
                            }
                        }
                    }

                    self.0.cur_row_index += 1;
                    return Some(GridRowRef::new(cells));
                } else {
                    self.0.cur_row_index = 0;

                    match self.0.read_gridbuffer() {
                        Ok(_) => self.next(),
                        Err(e) => {
                            error!("Failed to read gridbuffer, error: {}", e);
                            None
                        }
                    }
                }
            }
            None => {
                self.0.cur_row_index = 0;

                match self.0.read_gridbuffer() {
                    Ok(_) => self.next(),
                    Err(e) => {
                        error!("Failed to read gridbuffer, error: {}", e);
                        None
                    }
                }
            }
        }
    }
}

pub struct GridCellRef {
    pub gridbuffer: *const GridBuffer,
    pub row_index: usize,
    pub col_index: usize,
}

impl Default for GridCellRef {
    fn default() -> Self {
        Self::new(std::ptr::null(), 0, 0)
    }
}

impl GridCellRef {
    pub fn new(gridbuffer: *const GridBuffer, row_index: usize, col_index: usize) -> Self {
        Self {
            gridbuffer,
            row_index,
            col_index,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.gridbuffer.is_null()
    }

    pub fn get_sample_key(&self) -> Option<SampleKey> {
        unsafe {
            if self.gridbuffer.is_null() {
                None
            } else {
                let row = GridRow::new(self.gridbuffer, self.row_index);
                Some(row.get_sample_key())
            }
        }
    }
}

pub struct GridRowRef {
    pub cells: Vec<GridCellRef>,
    pub inner_row: Option<GridRow>,
}

impl GridRowRef {
    pub fn new(cells: Vec<GridCellRef>) -> Self {
        if cells.is_empty() {
            Self {
                cells,
                inner_row: None,
            }
        } else {
            let ptr = cells[0].gridbuffer;
            let row_index = cells[0].row_index;

            Self {
                cells,
                inner_row: Some(GridRow::new(ptr, row_index)),
            }
        }
    }
}

pub struct GridRowRefs {
    pub rows: Vec<GridRowRef>,
}

/// `LocalGridBufferMergeReader` is used to merge multiple tables.
pub struct LocalGridBufferMergeReader {
    /// Gridbuffer readers.
    readers: Vec<LocalGridRowReader>,

    /// Key ids.
    key_ids: Vec<Vec<u32>>,

    /// Total key ids.
    total_key_ids: usize,
}

impl LocalGridBufferMergeReader {
    pub fn new(readers: Vec<LocalGridRowReader>, key_ids: Vec<Vec<u32>>) -> Self {
        let total_key_ids = key_ids.iter().map(|k| k.len()).sum();

        Self {
            readers,
            key_ids,
            total_key_ids,
        }
    }
}

impl Iterator for LocalGridBufferMergeReader {
    type Item = GridRowRefs;

    /// TODO: Implement this.
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

pub struct LocalGridRowMergeReader(LocalGridBufferMergeReader);

impl LocalGridRowMergeReader {
    pub fn new(readers: Vec<LocalGridRowReader>, key_ids: Vec<Vec<u32>>) -> Self {
        Self(LocalGridBufferMergeReader::new(readers, key_ids))
    }
}

impl Iterator for LocalGridRowMergeReader {
    type Item = GridRowRef;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.total_key_ids == 0 {
            return None;
        }

        let mut cells = Vec::with_capacity(self.0.total_key_ids);

        let mut primary_key = None;

        match self.0.readers[0].next() {
            Some(row) => {
                if row.cells.len() == 0 {
                    error!("The number of cells of first reader is 0!");
                    return None;
                } else {
                    if row.cells[0].is_valid() {
                        primary_key = row.cells[0].get_sample_key();
                        cells.extend(row.cells);
                    } else {
                        error!("The first row has no inner row!");
                        return None;
                    }
                }
            }
            None => {
                error!("Read first reader failed!");
                return None;
            }
        }

        // Read other readers to match the first sample key. Stop until the sample key is found
        // or bigger than the first one.
        for i in 1..self.0.readers.len() {
            let mut has_value = false;

            while let Some(row) = self.0.readers[i].next() {
                if row.cells[0].is_valid() {
                    let cur_key = row.cells[0].get_sample_key();

                    match (cur_key.as_ref(), primary_key.as_ref()) {
                        (Some(key), Some(primary_key)) => {
                            if *key > *primary_key {
                                break;
                            } else if *key == *primary_key {
                                has_value = true;
                                cells.extend(row.cells);
                                break;
                            }
                        }
                        (_, _) => {
                            error!("Primary key is not set!");
                            return None;
                        }
                    }
                }
            }

            if !has_value {
                for j in 0..self.0.key_ids[i].len() {
                    cells.push(GridCellRef::default());
                }
            }
        }

        Some(GridRowRef::new(cells))
    }
}
