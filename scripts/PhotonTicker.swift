import Cocoa
import SQLite3

// Only show these — everything else is noise
let SIGNAL_TYPES = ["grinder", "staircase", "spring", "surge", "spring_ignition"]

struct Signal {
    let alertType: String
    let tokenAddress: String?
    let message: String
    let confidence: Int
}

func getSignals(dbPath: String) -> [Signal] {
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbPath, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL, nil) == SQLITE_OK else {
        return []
    }
    defer { sqlite3_close(db) }

    var signals: [Signal] = []
    var stmt: OpaquePointer?
    let query = """
        SELECT alert_type, token_address, message, confidence
        FROM alerts WHERE acknowledged = 0
            AND alert_type IN ('staircase','spring','surge','spring_ignition')
        ORDER BY confidence DESC
        LIMIT 5
    """
    if sqlite3_prepare_v2(db, query, -1, &stmt, nil) == SQLITE_OK {
        while sqlite3_step(stmt) == SQLITE_ROW {
            let atype = String(cString: sqlite3_column_text(stmt, 0))
            let token: String? = sqlite3_column_text(stmt, 1).map { String(cString: $0) }
            let msg = String(cString: sqlite3_column_text(stmt, 2))
            let conf = Int(sqlite3_column_int(stmt, 3))
            signals.append(Signal(alertType: atype, tokenAddress: token, message: msg, confidence: conf))
        }
    }
    sqlite3_finalize(stmt)
    return signals
}

func acknowledgeNoise(dbPath: String) {
    var db: OpaquePointer?
    guard sqlite3_open(dbPath, &db) == SQLITE_OK else { return }
    defer { sqlite3_close(db) }
    // Auto-clear the noise — only keep signal types unacknowledged
    sqlite3_exec(db, """
        UPDATE alerts SET acknowledged = 1
        WHERE acknowledged = 0
        AND alert_type NOT IN ('grinder','staircase','spring','surge','spring_ignition')
    """, nil, nil, nil)
}

func acknowledgeAll(dbPath: String) {
    var db: OpaquePointer?
    guard sqlite3_open(dbPath, &db) == SQLITE_OK else { return }
    defer { sqlite3_close(db) }
    sqlite3_exec(db, "UPDATE alerts SET acknowledged = 1 WHERE acknowledged = 0", nil, nil, nil)
}

func icon(for alertType: String) -> String {
    switch alertType {
    case "grinder": return "⛏️"
    case "staircase": return "📈"
    case "spring": return "🔋"
    case "surge": return "⚡"
    case "spring_ignition": return "🔥"
    default: return "•"
    }
}

func shortAddress(_ addr: String) -> String {
    if addr.count > 8 {
        return String(addr.prefix(4)) + "..." + String(addr.suffix(4))
    }
    return addr
}

func photonURL(for token: String) -> URL? {
    URL(string: "https://photon-sol.tinyastro.io/en/lp/\(token)")
}

// Extract a readable summary from the raw alert message
func readableSummary(_ msg: String, type: String, token: String?) -> String {
    let addr = token.map { shortAddress($0) } ?? "?"

    // Parse key numbers from the message
    if type == "staircase" {
        // "STAIRCASE xyz — momentum 80, distribution 53, multi-wave potential"
        if let mRange = msg.range(of: "momentum \\d+", options: .regularExpression) {
            let momentum = msg[mRange].replacingOccurrences(of: "momentum ", with: "")
            return "\(addr) — mom \(momentum)"
        }
    }
    if type == "spring" || type == "spring_ignition" {
        if let vRange = msg.range(of: "velocity [\\d.]+x", options: .regularExpression) {
            let vel = msg[vRange]
            return "\(addr) — \(vel)"
        }
    }
    if type == "surge" {
        return "\(addr) — surge"
    }

    return addr
}

class AppDelegate: NSObject, NSApplicationDelegate {
    var statusItem: NSStatusItem!
    var timer: Timer?
    let dbPath: String

    override init() {
        let home = FileManager.default.homeDirectoryForCurrentUser
        self.dbPath = home.appendingPathComponent("Code/photon/photon.db").path
        super.init()
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        refresh()

        // Auto-clear noise on startup
        acknowledgeNoise(dbPath: dbPath)

        timer = Timer.scheduledTimer(withTimeInterval: 15, repeats: true) { [weak self] _ in
            guard let self = self else { return }
            acknowledgeNoise(dbPath: self.dbPath)
            self.refresh()
        }
    }

    func refresh() {
        let signals = getSignals(dbPath: dbPath)

        // Menu bar title — clean
        if signals.isEmpty {
            statusItem.button?.title = "◉"
        } else {
            let best = signals[0]
            statusItem.button?.title = "\(icon(for: best.alertType)) \(signals.count)"
        }

        // Build menu
        let menu = NSMenu()

        if signals.isEmpty {
            let scanning = NSMenuItem(title: "Scanning...", action: nil, keyEquivalent: "")
            scanning.isEnabled = false
            menu.addItem(scanning)
        } else {
            for signal in signals {
                let summary = readableSummary(signal.message, type: signal.alertType, token: signal.tokenAddress)
                let title = "\(icon(for: signal.alertType)) [\(signal.confidence)] \(summary)"
                let item = NSMenuItem(title: title, action: #selector(signalClicked(_:)), keyEquivalent: "")
                item.target = self
                item.representedObject = signal.tokenAddress
                menu.addItem(item)
            }

            menu.addItem(NSMenuItem.separator())
            let clear = NSMenuItem(title: "Clear", action: #selector(clearAll), keyEquivalent: "c")
            clear.target = self
            menu.addItem(clear)
        }

        menu.addItem(NSMenuItem.separator())
        let quit = NSMenuItem(title: "Quit", action: #selector(quitApp), keyEquivalent: "q")
        quit.target = self
        menu.addItem(quit)

        statusItem.menu = menu
    }

    @objc func signalClicked(_ sender: NSMenuItem) {
        guard let token = sender.representedObject as? String,
              let url = photonURL(for: token) else { return }
        NSWorkspace.shared.open(url)
    }

    @objc func clearAll() {
        acknowledgeAll(dbPath: dbPath)
        refresh()
    }

    @objc func quitApp() {
        NSApplication.shared.terminate(nil)
    }
}

let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.setActivationPolicy(.accessory)
app.run()
