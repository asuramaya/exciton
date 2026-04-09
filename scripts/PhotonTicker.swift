import Cocoa
import SQLite3

// MARK: - Data

struct TokenSignal {
    let address: String
    let alertType: String
    let confidence: Int
    // From latest snapshot
    let topHolderPct: Double
    let top5Pct: Double
    let txRate: Double
    let velocity: Double
    let momentum: Int
    let distribution: Int
    let spring: Int
    let classification: String
    // Delta from previous snapshot
    let topHolderDelta: Double?
    let momentumDelta: Int?
    let timeSinceLast: Int64?
}

let DB_PATH: String = {
    let home = FileManager.default.homeDirectoryForCurrentUser
    return home.appendingPathComponent("Code/photon/photon.db").path
}()

// MARK: - Database queries

func fetchSignals() -> [TokenSignal] {
    var db: OpaquePointer?
    guard sqlite3_open_v2(DB_PATH, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL, nil) == SQLITE_OK else {
        return []
    }
    defer { sqlite3_close(db) }

    // Get signal-worthy alerts
    let query = """
        SELECT alert_type, token_address, confidence
        FROM alerts
        WHERE acknowledged = 0
            AND alert_type IN ('grinder','staircase','spring','surge','spring_ignition')
        ORDER BY confidence DESC
        LIMIT 5
    """

    var stmt: OpaquePointer?
    var signals: [TokenSignal] = []

    guard sqlite3_prepare_v2(db, query, -1, &stmt, nil) == SQLITE_OK else { return [] }

    while sqlite3_step(stmt) == SQLITE_ROW {
        let alertType = String(cString: sqlite3_column_text(stmt, 0))
        let address = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
        let confidence = Int(sqlite3_column_int(stmt, 2))

        // Fetch latest snapshot for this token
        var topHolder = 0.0, top5 = 0.0, txRate = 0.0, velocity = 0.0
        var momentum = 0, distribution = 0, spring = 0
        var classification = alertType.uppercased()

        var sStmt: OpaquePointer?
        if sqlite3_prepare_v2(db,
            "SELECT top_holder_pct, top5_pct, tx_rate, velocity, momentum, distribution, spring, classification FROM token_snapshots WHERE token_address = ? ORDER BY timestamp DESC LIMIT 1",
            -1, &sStmt, nil) == SQLITE_OK {
            sqlite3_bind_text(sStmt, 1, address, -1, nil)
            if sqlite3_step(sStmt) == SQLITE_ROW {
                topHolder = sqlite3_column_double(sStmt, 0)
                top5 = sqlite3_column_double(sStmt, 1)
                txRate = sqlite3_column_double(sStmt, 2)
                velocity = sqlite3_column_double(sStmt, 3)
                momentum = Int(sqlite3_column_int(sStmt, 4))
                distribution = Int(sqlite3_column_int(sStmt, 5))
                spring = Int(sqlite3_column_int(sStmt, 6))
                classification = sqlite3_column_text(sStmt, 7).map { String(cString: $0) } ?? classification
            }
        }
        sqlite3_finalize(sStmt)

        // Fetch previous snapshot for delta
        var thDelta: Double? = nil
        var momDelta: Int? = nil
        var elapsed: Int64? = nil

        var dStmt: OpaquePointer?
        if sqlite3_prepare_v2(db,
            "SELECT top_holder_pct, momentum, timestamp FROM token_snapshots WHERE token_address = ? ORDER BY timestamp DESC LIMIT 1 OFFSET 1",
            -1, &dStmt, nil) == SQLITE_OK {
            sqlite3_bind_text(dStmt, 1, address, -1, nil)
            if sqlite3_step(dStmt) == SQLITE_ROW {
                let prevTop = sqlite3_column_double(dStmt, 0)
                let prevMom = Int(sqlite3_column_int(dStmt, 1))
                thDelta = topHolder - prevTop
                momDelta = momentum - prevMom
            }
        }
        sqlite3_finalize(dStmt)

        signals.append(TokenSignal(
            address: address,
            alertType: alertType,
            confidence: confidence,
            topHolderPct: topHolder,
            top5Pct: top5,
            txRate: txRate,
            velocity: velocity,
            momentum: momentum,
            distribution: distribution,
            spring: spring,
            classification: classification,
            topHolderDelta: thDelta,
            momentumDelta: momDelta,
            timeSinceLast: elapsed
        ))
    }
    sqlite3_finalize(stmt)

    return signals
}

func acknowledgeNoise() {
    var db: OpaquePointer?
    guard sqlite3_open(DB_PATH, &db) == SQLITE_OK else { return }
    defer { sqlite3_close(db) }
    sqlite3_exec(db, """
        UPDATE alerts SET acknowledged = 1
        WHERE acknowledged = 0
        AND alert_type NOT IN ('grinder','staircase','spring','surge','spring_ignition')
    """, nil, nil, nil)
}

func acknowledgeAll() {
    var db: OpaquePointer?
    guard sqlite3_open(DB_PATH, &db) == SQLITE_OK else { return }
    defer { sqlite3_close(db) }
    sqlite3_exec(db, "UPDATE alerts SET acknowledged = 1 WHERE acknowledged = 0", nil, nil, nil)
}

// MARK: - Formatting

func classIcon(_ classification: String) -> String {
    switch classification {
    case "GRINDER": return "⛏️"
    case "STAIRCASE": return "📈"
    case "SPRING": return "🔋"
    case "SURGE": return "⚡"
    default: return "•"
    }
}

func shortAddr(_ addr: String) -> String {
    guard addr.count > 10 else { return addr }
    return String(addr.prefix(4)) + ".." + String(addr.suffix(4))
}

func formatDelta(_ val: Double) -> String {
    if val > 0 { return "+\(String(format: "%.1f", val))%" }
    if val < 0 { return "\(String(format: "%.1f", val))%" }
    return "—"
}

func directionArrow(_ delta: Double?) -> String {
    guard let d = delta else { return "" }
    if d < -1.0 { return " ↘" }  // distributing
    if d > 1.0 { return " ↗" }   // concentrating (bad)
    return ""
}

func photonURL(_ token: String) -> URL? {
    URL(string: "https://photon-sol.tinyastro.io/en/lp/\(token)")
}

// MARK: - Menu building

func buildSignalItem(_ s: TokenSignal) -> NSMenuItem {
    let addr = shortAddr(s.address)
    let arrow = directionArrow(s.topHolderDelta)
    let velStr = String(format: "%.1fx", s.velocity)
    let holderStr = String(format: "%.1f%%", s.topHolderPct)

    // Primary line: icon [score] addr — key metric
    let title = "\(classIcon(s.classification)) \(addr)  \(holderStr)\(arrow)  \(velStr)  [\(s.confidence)]"

    let item = NSMenuItem(title: title, action: #selector(AppDelegate.signalClicked(_:)), keyEquivalent: "")
    item.representedObject = s.address

    return item
}

func buildDetailItems(_ s: TokenSignal) -> [NSMenuItem] {
    var items: [NSMenuItem] = []

    let metrics = "     mom \(s.momentum)  dist \(s.distribution)  spr \(s.spring)  tx \(String(format: "%.0f", s.txRate))/m"
    let mItem = NSMenuItem(title: metrics, action: nil, keyEquivalent: "")
    mItem.isEnabled = false
    items.append(mItem)

    if let thDelta = s.topHolderDelta {
        let direction = thDelta < -1.0 ? "distributing" : thDelta > 1.0 ? "concentrating ⚠" : "stable"
        let deltaLine = "     holder \(formatDelta(thDelta))  \(direction)"
        let dItem = NSMenuItem(title: deltaLine, action: nil, keyEquivalent: "")
        dItem.isEnabled = false
        items.append(dItem)
    }

    return items
}

// MARK: - App

class AppDelegate: NSObject, NSApplicationDelegate {
    var statusItem: NSStatusItem!
    var timer: Timer?

    func applicationDidFinishLaunching(_ notification: Notification) {
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        acknowledgeNoise()
        refresh()

        timer = Timer.scheduledTimer(withTimeInterval: 15, repeats: true) { [weak self] _ in
            acknowledgeNoise()
            self?.refresh()
        }
    }

    func refresh() {
        let signals = fetchSignals()

        // Menu bar — minimal
        if signals.isEmpty {
            statusItem.button?.title = "◉"
        } else {
            let best = signals[0]
            let arrow = directionArrow(best.topHolderDelta)
            statusItem.button?.title = "\(classIcon(best.classification))\(signals.count)\(arrow)"
        }

        let menu = NSMenu()

        if signals.isEmpty {
            let item = NSMenuItem(title: "Scanning...", action: nil, keyEquivalent: "")
            item.isEnabled = false
            menu.addItem(item)
        } else {
            for (i, signal) in signals.enumerated() {
                let item = buildSignalItem(signal)
                item.target = self
                menu.addItem(item)

                // Detail rows under each signal
                for detail in buildDetailItems(signal) {
                    menu.addItem(detail)
                }

                if i < signals.count - 1 {
                    menu.addItem(NSMenuItem.separator())
                }
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
              let url = photonURL(token) else { return }
        NSWorkspace.shared.open(url)
    }

    @objc func clearAll() {
        acknowledgeAll()
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
