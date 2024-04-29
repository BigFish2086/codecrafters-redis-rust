pub const CR: u8 = b'\r';
pub const LF: u8 = b'\n';

pub const DEFAULT_PORT: u16 = 6379;

pub const MAGIC: &str = "REDIS";
pub const MAGIC_BYTES: usize = MAGIC.len();
pub const VERSION_BYTES: usize = 4;
pub const TIME_SECS_BYTES: usize = 4;
pub const TIME_MILLIS_BYTES: usize = 8;

pub const COMPRESS_AT_LENGTH: u16 = 150;

pub const SLAVE_LIFETIME_LIMIT: usize = 3;

macro_rules! rdb_opcode {
    ( $( ($opcode:expr, $konst:ident);)+) => {
        $( pub const $konst: u8 = $opcode; )+
    }
}

rdb_opcode! {
    (0xFF, EOF);
    (0xFE, SELECTDB);
    (0xFD, EXPIRETIME);
    (0xFC, EXPIRETIMEMS);
    (0xFB, RESIZEDB);
    (0xFA, AUX);
}

