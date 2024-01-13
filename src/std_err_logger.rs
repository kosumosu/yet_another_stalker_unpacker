use log::Level;

pub struct StdErrLogger {
    max_level: Level
}

impl StdErrLogger {
    pub const fn new(max_level: Level) -> StdErrLogger {
        StdErrLogger { max_level }
    }
}


impl log::Log for StdErrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= self.max_level
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{}", record.args());
        }
    }

    fn flush(&self) {}
}
