import Cocoa
import SQLite3

struct Alert {
    let alertType: String
    let tokenAddress: String?
    let message: String
    let confidence: Int
}

func getAlerts(dbPath: String, limit: Int = 10) -> (total: Int, alerts: [Alert], typeCounts: [(String, Int)]) {
    var db: OpaquePointer?
    guard sqlite3_open_v2(dbPath, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL, nil) == SQLITE_OK else {
        return (0, [], [])
    }
    defer { sqlite3_close(db) }

    var stmt: OpaquePointer?
    var total = 0
    if sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0", -1, &stmt, nil) == SQLITE_OK {
        if sqlite3_step(stmt) == SQLITE_ROW {
            total = Int(sqlite3_column_int(stmt, 0))
        }
    }
    sqlite3_finalize(stmt)

    if total == 0 { return (0, [], []) }

    var typeCounts: [(String, Int)] = []
    if sqlite3_prepare_v2(db, "SELECT alert_type, COUNT(*) FROM alerts WHERE acknowledged = 0 GROUP BY alert_type ORDER BY COUNT(*) DESC", -1, &stmt, nil) == SQLITE_OK {
        while sqlite3_step(stmt) == SQLITE_ROW {
            let atype = String(cString: sqlite3_column_text(stmt, 0))
            let count = Int(sqlite3_column_int(stmt, 1))
            typeCounts.append((atype, count))
        }
    }
    sqlite3_finalize(stmt)

    var alerts: [Alert] = []
    let query = """
        SELECT alert_type, token_address, message, confidence
        FROM alerts WHERE acknowledged = 0
        ORDER BY
            CASE WHEN alert_type IN ('staircase','spring','surge','classification_change','spring_ignition') THEN 0 ELSE 1 END,
            confidence DESC
        LIMIT ?
    """
    if sqlite3_prepare_v2(db, query, -1, &stmt, nil) == SQLITE_OK {
        sqlite3_bind_int(stmt, 1, Int32(limit))
        while sqlite3_step(stmt) == SQLITE_ROW {
            let atype = String(cString: sqlite3_column_text(stmt, 0))
            let token: String? = sqlite3_column_text(stmt, 1).map { String(cString: $0) }
            let msg = String(cString: sqlite3_column_text(stmt, 2))
            let conf = Int(sqlite3_column_int(stmt, 3))
            alerts.append(Alert(alertType: atype, tokenAddress: token, message: msg, confidence: conf))
        }
    }
    sqlite3_finalize(stmt)

    return (total, alerts, typeCounts)
}

func acknowledgeAlerts(dbPath: String) {
    var db: OpaquePointer?
    guard sqlite3_open(dbPath, &db) == SQLITE_OK else { return }
    defer { sqlite3_close(db) }
    sqlite3_exec(db, "UPDATE alerts SET acknowledged = 1 WHERE acknowledged = 0", nil, nil, nil)
}

func icon(for alertType: String) -> String {
    switch alertType {
    case "staircase": return "📈"
    case "spring": return "🔋"
    case "surge": return "⚡"
    case "spring_ignition": return "🔥"
    case "classification_change": return "🔄"
    case "concentrating": return "⚠️"
    case "velocity_crash": return "📉"
    case "active_trap": return "🪤"
    case "developing": return "🌱"
    case "danger": return "💀"
    default: return "•"
    }
}

func photonURL(for token: String) -> URL? {
    URL(string: "https://photon-sol.tinyastro.io/en/lp/\(token)")
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
        updateMenu()
        timer = Timer.scheduledTimer(withTimeInterval: 15, repeats: true) { [weak self] _ in
            self?.updateMenu()
        }
    }

    func updateMenu() {
        let (total, alerts, typeCounts) = getAlerts(dbPath: dbPath)

        if total == 0 {
            statusItem.button?.title = "◉ Photon"
        } else {
            let hasHigh = typeCounts.contains { ["staircase", "spring", "surge", "spring_ignition"].contains($0.0) }
            statusItem.button?.title = "\(hasHigh ? "🔥" : "◉") \(total)"
        }

        let menu = NSMenu()

        if total == 0 {
            menu.addItem(NSMenuItem(title: "No pending alerts", action: nil, keyEquivalent: ""))
            menu.addItem(NSMenuItem(title: "Scanner running...", action: nil, keyEquivalent: ""))
        } else {
            let summary = NSMenuItem(title: "[\(total) alerts]", action: nil, keyEquivalent: "")
            summary.isEnabled = false
            menu.addItem(summary)

            for (atype, count) in typeCounts {
                let item = NSMenuItem(title: "  \(icon(for: atype)) \(atype): \(count)", action: nil, keyEquivalent: "")
                item.isEnabled = false
                menu.addItem(item)
            }

            menu.addItem(NSMenuItem.separator())

            for alert in alerts.prefix(8) {
                let short = alert.message.count > 70 ? String(alert.message.prefix(67)) + "..." : alert.message
                let title = "\(icon(for: alert.alertType)) [\(alert.confidence)] \(short)"
                let item = NSMenuItem(title: title, action: #selector(alertClicked(_:)), keyEquivalent: "")
                item.target = self
                item.representedObject = alert.tokenAddress
                menu.addItem(item)
            }

            menu.addItem(NSMenuItem.separator())
            let clear = NSMenuItem(title: "Clear all alerts", action: #selector(clearAlerts), keyEquivalent: "c")
            clear.target = self
            menu.addItem(clear)
        }

        menu.addItem(NSMenuItem.separator())
        let quit = NSMenuItem(title: "Quit Photon Ticker", action: #selector(quitApp), keyEquivalent: "q")
        quit.target = self
        menu.addItem(quit)

        statusItem.menu = menu
    }

    @objc func alertClicked(_ sender: NSMenuItem) {
        guard let token = sender.representedObject as? String,
              let url = photonURL(for: token) else { return }
        NSWorkspace.shared.open(url)
    }

    @objc func clearAlerts() {
        acknowledgeAlerts(dbPath: dbPath)
        updateMenu()
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
