pub struct StdErrLogger;

impl StdErrLogger {
    pub const fn new() -> StdErrLogger {
        StdErrLogger
    }
}


impl log::Log for StdErrLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{}", record.args());
        }
    }

    fn flush(&self) {}
}
