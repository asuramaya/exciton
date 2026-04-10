import Cocoa
import SQLite3

// MARK: - Data

struct TokenSignal {
    let address: String
    let alertType: String
    let confidence: Int
    let topHolderPct: Double
    let top5Pct: Double
    let txRate: Double
    let velocity: Double
    let momentum: Int
    let distribution: Int
    let spring: Int
    let classification: String
    let topHolderDelta: Double?
    let momentumDelta: Int?
}

let DB_PATH: String = {
    FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent("Code/photon/photon.db").path
}()

// MARK: - DB

func fetchSignals() -> [TokenSignal] {
    var db: OpaquePointer?
    guard sqlite3_open_v2(DB_PATH, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL, nil) == SQLITE_OK else { return [] }
    defer { sqlite3_close(db) }

    var stmt: OpaquePointer?
    let query = "SELECT alert_type, token_address, confidence FROM alerts WHERE acknowledged = 0 AND alert_type IN ('grinder','staircase','spring','surge','spring_ignition') ORDER BY confidence DESC LIMIT 5"
    guard sqlite3_prepare_v2(db, query, -1, &stmt, nil) == SQLITE_OK else { return [] }

    var signals: [TokenSignal] = []
    while sqlite3_step(stmt) == SQLITE_ROW {
        let alertType = String(cString: sqlite3_column_text(stmt, 0))
        let address = sqlite3_column_text(stmt, 1).map { String(cString: $0) } ?? ""
        let confidence = Int(sqlite3_column_int(stmt, 2))

        var topHolder = 0.0, top5 = 0.0, txRate = 0.0, velocity = 0.0
        var momentum = 0, distribution = 0, spring = 0
        var classification = alertType.uppercased()

        var sStmt: OpaquePointer?
        if sqlite3_prepare_v2(db, "SELECT top_holder_pct, top5_pct, tx_rate, velocity, momentum, distribution, spring, classification FROM token_snapshots WHERE token_address = '\(address)' ORDER BY timestamp DESC LIMIT 1", -1, &sStmt, nil) == SQLITE_OK {
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

        var thDelta: Double? = nil, momDelta: Int? = nil
        var dStmt: OpaquePointer?
        if sqlite3_prepare_v2(db, "SELECT top_holder_pct, momentum FROM token_snapshots WHERE token_address = '\(address)' ORDER BY timestamp DESC LIMIT 1 OFFSET 1", -1, &dStmt, nil) == SQLITE_OK {
            if sqlite3_step(dStmt) == SQLITE_ROW {
                thDelta = topHolder - sqlite3_column_double(dStmt, 0)
                momDelta = momentum - Int(sqlite3_column_int(dStmt, 1))
            }
        }
        sqlite3_finalize(dStmt)

        signals.append(TokenSignal(address: address, alertType: alertType, confidence: confidence,
            topHolderPct: topHolder, top5Pct: top5, txRate: txRate, velocity: velocity,
            momentum: momentum, distribution: distribution, spring: spring, classification: classification,
            topHolderDelta: thDelta, momentumDelta: momDelta))
    }
    sqlite3_finalize(stmt)
    return signals
}

func acknowledgeNoise() {
    var db: OpaquePointer?
    guard sqlite3_open(DB_PATH, &db) == SQLITE_OK else { return }
    defer { sqlite3_close(db) }
    sqlite3_exec(db, "UPDATE alerts SET acknowledged = 1 WHERE acknowledged = 0 AND alert_type NOT IN ('grinder','staircase','spring','surge','spring_ignition')", nil, nil, nil)
}

func acknowledgeAll() {
    var db: OpaquePointer?
    guard sqlite3_open(DB_PATH, &db) == SQLITE_OK else { return }
    defer { sqlite3_close(db) }
    sqlite3_exec(db, "UPDATE alerts SET acknowledged = 1 WHERE acknowledged = 0", nil, nil, nil)
}

// MARK: - Formatting

func shortAddr(_ addr: String) -> String {
    guard addr.count > 12 else { return addr }
    return String(addr.prefix(6)) + "…" + String(addr.suffix(4))
}

func photonURL(_ token: String) -> URL? {
    URL(string: "https://photon-sol.tinyastro.io/en/lp/\(token)")
}

func classColor(_ c: String) -> NSColor {
    switch c {
    case "GRINDER": return NSColor(red: 0.2, green: 0.9, blue: 0.4, alpha: 1)
    case "STAIRCASE": return NSColor(red: 0.3, green: 0.8, blue: 1.0, alpha: 1)
    case "SPRING": return NSColor(red: 0.6, green: 0.4, blue: 1.0, alpha: 1)
    case "SURGE": return NSColor(red: 1.0, green: 0.6, blue: 0.2, alpha: 1)
    default: return NSColor.secondaryLabelColor
    }
}

func classIcon(_ c: String) -> String {
    switch c {
    case "GRINDER": return "⛏"
    case "STAIRCASE": return "📈"
    case "SPRING": return "🔋"
    case "SURGE": return "⚡"
    default: return "•"
    }
}

func directionSymbol(_ delta: Double?) -> (String, NSColor) {
    guard let d = delta else { return ("—", .tertiaryLabelColor) }
    if d < -2.0 { return ("↘ distributing", NSColor(red: 0.2, green: 0.9, blue: 0.4, alpha: 1)) }
    if d < -0.5 { return ("↘ slow dist", NSColor(red: 0.5, green: 0.8, blue: 0.5, alpha: 1)) }
    if d > 2.0 { return ("↗ concentrating", NSColor(red: 1.0, green: 0.3, blue: 0.3, alpha: 1)) }
    if d > 0.5 { return ("↗ slight conc", NSColor(red: 1.0, green: 0.6, blue: 0.4, alpha: 1)) }
    return ("→ stable", .tertiaryLabelColor)
}

func confidenceBar(_ conf: Int) -> String {
    let filled = conf / 10
    return String(repeating: "█", count: filled) + String(repeating: "░", count: 10 - filled)
}

// MARK: - Custom Menu Item View

class SignalMenuView: NSView {
    let signal: TokenSignal
    var trackingArea: NSTrackingArea?
    var isHovered = false
    var onClick: (() -> Void)?

    init(signal: TokenSignal, width: CGFloat) {
        self.signal = signal
        super.init(frame: NSRect(x: 0, y: 0, width: width, height: 68))
    }

    required init?(coder: NSCoder) { fatalError() }

    override func updateTrackingAreas() {
        if let existing = trackingArea { removeTrackingArea(existing) }
        trackingArea = NSTrackingArea(rect: bounds, options: [.mouseEnteredAndExited, .activeAlways], owner: self)
        addTrackingArea(trackingArea!)
    }

    override func mouseEntered(with event: NSEvent) { isHovered = true; needsDisplay = true }
    override func mouseExited(with event: NSEvent) { isHovered = false; needsDisplay = true }
    override func mouseUp(with event: NSEvent) { onClick?() }

    override func draw(_ dirtyRect: NSRect) {
        if isHovered {
            NSColor.selectedContentBackgroundColor.withAlphaComponent(0.3).setFill()
            bounds.fill()
        }

        let s = signal
        let color = classColor(s.classification)
        let (dirStr, dirColor) = directionSymbol(s.topHolderDelta)

        // Row 1: Classification badge + address + confidence
        let row1 = NSMutableAttributedString()
        row1.append(NSAttributedString(string: "\(classIcon(s.classification)) ", attributes: [.font: NSFont.systemFont(ofSize: 13)]))
        row1.append(NSAttributedString(string: s.classification, attributes: [
            .font: NSFont.boldSystemFont(ofSize: 11),
            .foregroundColor: color
        ]))
        row1.append(NSAttributedString(string: "  \(shortAddr(s.address))", attributes: [
            .font: NSFont.monospacedSystemFont(ofSize: 11, weight: .regular),
            .foregroundColor: NSColor.secondaryLabelColor
        ]))
        let confStr = "  \(s.confidence)"
        row1.append(NSAttributedString(string: confStr, attributes: [
            .font: NSFont.monospacedDigitSystemFont(ofSize: 12, weight: .bold),
            .foregroundColor: s.confidence >= 65 ? color : NSColor.tertiaryLabelColor
        ]))
        row1.draw(at: NSPoint(x: 14, y: 46))

        // Row 2: Key metrics bar
        let holderStr = String(format: "%.1f%%", s.topHolderPct)
        let velStr = String(format: "%.1fx", s.velocity)
        let rateStr = s.txRate >= 1.0 ? String(format: "%.0f/m", s.txRate) : String(format: "%.1f/m", s.txRate)

        let row2 = NSMutableAttributedString()
        // Holder %
        let holderColor = s.topHolderPct < 15 ? NSColor(red: 0.2, green: 0.9, blue: 0.4, alpha: 1) :
                          s.topHolderPct < 30 ? NSColor(red: 0.8, green: 0.8, blue: 0.3, alpha: 1) :
                          NSColor(red: 1.0, green: 0.4, blue: 0.3, alpha: 1)
        row2.append(NSAttributedString(string: "⬤ ", attributes: [.font: NSFont.systemFont(ofSize: 6), .foregroundColor: holderColor]))
        row2.append(NSAttributedString(string: holderStr, attributes: [.font: NSFont.monospacedDigitSystemFont(ofSize: 11, weight: .medium), .foregroundColor: holderColor]))
        row2.append(NSAttributedString(string: "   ", attributes: [:]))
        // Velocity
        let velColor = s.velocity >= 2.0 ? NSColor(red: 0.2, green: 0.9, blue: 0.4, alpha: 1) :
                       s.velocity >= 1.0 ? NSColor.labelColor : NSColor.tertiaryLabelColor
        row2.append(NSAttributedString(string: velStr, attributes: [.font: NSFont.monospacedDigitSystemFont(ofSize: 11, weight: .medium), .foregroundColor: velColor]))
        row2.append(NSAttributedString(string: "   ", attributes: [:]))
        // Tx rate
        row2.append(NSAttributedString(string: rateStr, attributes: [.font: NSFont.monospacedDigitSystemFont(ofSize: 11, weight: .regular), .foregroundColor: NSColor.secondaryLabelColor]))
        row2.draw(at: NSPoint(x: 20, y: 26))

        // Row 3: Direction + momentum bar
        let row3 = NSMutableAttributedString()
        row3.append(NSAttributedString(string: dirStr, attributes: [.font: NSFont.systemFont(ofSize: 10), .foregroundColor: dirColor]))
        row3.append(NSAttributedString(string: "   m\(s.momentum) d\(s.distribution) s\(s.spring)", attributes: [
            .font: NSFont.monospacedDigitSystemFont(ofSize: 9, weight: .regular),
            .foregroundColor: NSColor.tertiaryLabelColor
        ]))
        row3.draw(at: NSPoint(x: 20, y: 8))
    }
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

        // Menu bar title
        if signals.isEmpty {
            statusItem.button?.title = "◉"
        } else {
            let best = signals[0]
            let (dirStr, _) = directionSymbol(best.topHolderDelta)
            let arrow = dirStr.hasPrefix("↘") ? "↘" : dirStr.hasPrefix("↗") ? "↗" : ""
            statusItem.button?.title = "\(classIcon(best.classification))\(signals.count)\(arrow)"
        }

        let menu = NSMenu()
        menu.minimumWidth = 320

        if signals.isEmpty {
            let item = NSMenuItem(title: "  Scanning…", action: nil, keyEquivalent: "")
            item.isEnabled = false
            menu.addItem(item)
        } else {
            for signal in signals {
                let item = NSMenuItem()
                let view = SignalMenuView(signal: signal, width: 320)
                view.onClick = { [weak self] in
                    if let url = photonURL(signal.address) {
                        NSWorkspace.shared.open(url)
                    }
                    self?.statusItem.menu?.cancelTracking()
                }
                item.view = view
                menu.addItem(item)
                menu.addItem(NSMenuItem.separator())
            }

            let clear = NSMenuItem(title: "  Clear", action: #selector(clearAll), keyEquivalent: "c")
            clear.target = self
            menu.addItem(clear)
        }

        menu.addItem(NSMenuItem.separator())
        let quit = NSMenuItem(title: "  Quit", action: #selector(quitApp), keyEquivalent: "q")
        quit.target = self
        menu.addItem(quit)

        statusItem.menu = menu
    }

    @objc func clearAll() { acknowledgeAll(); refresh() }
    @objc func quitApp() { NSApplication.shared.terminate(nil) }
}

let app = NSApplication.shared
let delegate = AppDelegate()
app.delegate = delegate
app.setActivationPolicy(.accessory)
app.run()
