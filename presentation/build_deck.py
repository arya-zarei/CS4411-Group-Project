"""Build the 15-minute presentation deck (python-pptx) with speaker notes.

    uv run --no-project --with python-pptx --with pillow python presentation/build_deck.py

Writes presentation/group6-final-presentation.pptx. Figures come from results/figures/ and the live
dashboard screenshot from presentation/assets/. Speaker notes hold the script, with the speaker's name
and the time budget at the top of each slide's notes.
"""

from __future__ import annotations

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

ROOT = Path(__file__).resolve().parent.parent
FIG = ROOT / "results" / "figures"
ASSETS = ROOT / "presentation" / "assets"
OUT = ROOT / "presentation" / "group6-final-presentation.pptx"

# Palette: the same colors as the result figures.
BLUE, ORANGE, AQUA = RGBColor(0x2A, 0x78, 0xD6), RGBColor(0xEB, 0x68, 0x34), RGBColor(0x1B, 0xAF, 0x7A)
INK, INK2 = RGBColor(0x1B, 0x1F, 0x24), RGBColor(0x5A, 0x62, 0x6B)
SURFACE, PANEL, LINE = RGBColor(0xFC, 0xFC, 0xFB), RGBColor(0xF0, 0xF2, 0xF5), RGBColor(0xD5, 0xDA, 0xE0)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
FONT = "Calibri"

prs = Presentation()
prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
BLANK = prs.slide_layouts[6]
W, H = prs.slide_width, prs.slide_height
_count = [0]


def text(slide, x, y, w, h, content, size=18, bold=False, color=INK, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP):
    """Add a text box. `content` is a string or a list of strings (one paragraph each)."""
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(0.05)
    tf.margin_top = tf.margin_bottom = Inches(0.03)
    for i, line in enumerate(content if isinstance(content, list) else [content]):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        r = p.add_run()
        r.text = line
        r.font.size, r.font.bold, r.font.color.rgb, r.font.name = Pt(size), bold, color, FONT
    return tb


def bullets(slide, x, y, w, h, items, size=18, gap=8, color=INK):
    """Bulleted list; an item may be (bold_lead, rest)."""
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    for i, item in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.space_after = Pt(gap)
        lead, rest = item if isinstance(item, tuple) else (None, item)
        dot = p.add_run()
        dot.text = "■  "
        dot.font.size, dot.font.color.rgb, dot.font.name = Pt(size - 6), BLUE, FONT
        if lead:
            r = p.add_run()
            r.text = lead + " "
            r.font.size, r.font.bold, r.font.color.rgb, r.font.name = Pt(size), True, color, FONT
        r = p.add_run()
        r.text = rest
        r.font.size, r.font.color.rgb, r.font.name = Pt(size), color, FONT
    return tb


def box(slide, x, y, w, h, label, fill=PANEL, line=LINE, color=INK, size=14, bold=False, shape=MSO_SHAPE.ROUNDED_RECTANGLE):
    s = slide.shapes.add_shape(shape, x, y, w, h)
    s.fill.solid()
    s.fill.fore_color.rgb = fill
    s.line.color.rgb = line
    s.line.width = Pt(1.25)
    s.shadow.inherit = False
    tf = s.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    tf.margin_left = tf.margin_right = Inches(0.06)
    for i, line_text in enumerate(label if isinstance(label, list) else [label]):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = line_text
        r.font.size, r.font.bold, r.font.color.rgb, r.font.name = Pt(size), bold and i == 0, color, FONT
    return s


def arrow(slide, x1, y1, x2, y2, color=INK2, dashed=False):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, x1, y1, x2, y2)
    c.line.color.rgb = color
    c.line.width = Pt(2)
    ln = c.line._get_or_add_ln()
    tail = ln.makeelement("{http://schemas.openxmlformats.org/drawingml/2006/main}tailEnd", {"type": "triangle"})
    ln.append(tail)
    if dashed:
        c.line.dash_style = 4  # dash
    return c


def new_slide(title, notes, speaker, seconds):
    """A content slide: accent bar, claim-style title, footer. Notes start with the speaker and time budget."""
    s = prs.slides.add_slide(BLANK)
    _count[0] += 1
    bg = s.background.fill
    bg.solid()
    bg.fore_color.rgb = SURFACE
    bar = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(0.18), H)
    bar.fill.solid()
    bar.fill.fore_color.rgb = BLUE
    bar.line.fill.background()
    text(s, Inches(0.6), Inches(0.35), Inches(12.2), Inches(1.0), title, size=30, bold=True, anchor=MSO_ANCHOR.MIDDLE)
    text(s, Inches(0.6), Inches(7.05), Inches(9), Inches(0.3),
         "Group 6  |  COMPSCI 4411/9538  |  Kafka + Spark market-data pipeline", size=10, color=INK2)
    text(s, Inches(12.2), Inches(7.05), Inches(0.7), Inches(0.3), str(_count[0] + 1), size=10, color=INK2, align=PP_ALIGN.RIGHT)
    words = len(notes.split())
    est = 150 if notes.startswith("DEMO SCRIPT") else max(10, round(words / 140 * 60 / 5) * 5)  # speaking pace: 140 words/min
    s.notes_slide.notes_text_frame.text = f"[{speaker}, about {est} s]\n\n{notes}"
    return s


def picture(slide, name, x, y, w=None, h=None):
    path = (FIG / name) if (FIG / name).exists() else (ASSETS / name)
    kw = {}
    if w is not None:
        kw["width"] = w
    if h is not None:
        kw["height"] = h
    return slide.shapes.add_picture(str(path), x, y, **kw)


def source(slide, content):
    text(slide, Inches(0.6), Inches(6.7), Inches(12.2), Inches(0.35), content, size=11, color=INK2)


# ---------------------------------------------------------------------------------------------- 1
s = prs.slides.add_slide(BLANK)
bg = s.background.fill
bg.solid()
bg.fore_color.rgb = INK
band = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, Inches(5.2), W, Inches(0.12))
band.fill.solid()
band.fill.fore_color.rgb = BLUE
band.line.fill.background()
text(s, Inches(0.9), Inches(1.5), Inches(11.5), Inches(2.4),
     ["Do Kafka and Spark keep their promises?", "A market-data pipeline under load and failure"], size=40, bold=True, color=WHITE)
text(s, Inches(0.9), Inches(3.95), Inches(11.5), Inches(0.9),
     "Real-time bars and alerts from 100 symbols, 3 Kafka brokers, Spark Structured Streaming, PostgreSQL and Grafana",
     size=20, color=RGBColor(0xC9, 0xD1, 0xDA))
text(s, Inches(0.9), Inches(5.55), Inches(11.5), Inches(1.2),
     ["Group 6: Arya Zarei, Rawad Alharastani, Mark Samwaiel", "COMPSCI 4411/9538, Databases 2"], size=20, color=WHITE)
s.notes_slide.notes_text_frame.text = "[Arya, about 25 s]\n\n" + 'Hi, we are Group 6. In fifteen minutes we will show a real-time pipeline that turns trades into bars and alerts, and test whether Kafka and Spark keep their promises of speed, durability and exactly-once results. Rawad covers Spark, Mark runs the demo and most of the results, and I start with Kafka and finish with what we learned.'

# ---------------------------------------------------------------------------------------------- 2
s = new_slide(
    "Trades arrive every millisecond; desks need bars and alerts within seconds",
    'A trading desk does not want raw trades. It wants a summary per symbol per minute, and a warning when a price moves more than usual. The usual design is a durable log plus a stream processor: Kafka and Spark Structured Streaming. Both come with promises. Kafka promises speed and durability through replication. Spark promises exactly-once results through replay and checkpoints. Those promises depend on configuration, so we asked three questions: how do partitions, trigger interval and processing mode trade latency against throughput; what happens to the data when a broker or Spark crashes; and what does exactly-once output cost?',
    "Arya", 55,
)
box(s, Inches(0.6), Inches(1.6), Inches(3.6), Inches(1.25), ["Trades", "100 symbols"], fill=PANEL, size=18, bold=True)
box(s, Inches(4.75), Inches(1.6), Inches(3.6), Inches(1.25), ["Bars + alerts", "1 minute, 3 sigma"], fill=PANEL, size=18, bold=True)
box(s, Inches(8.9), Inches(1.6), Inches(3.6), Inches(1.25), ["Dashboard", "seconds later"], fill=PANEL, size=18, bold=True)
arrow(s, Inches(4.2), Inches(2.22), Inches(4.75), Inches(2.22))
arrow(s, Inches(8.35), Inches(2.22), Inches(8.9), Inches(2.22))
text(s, Inches(0.6), Inches(3.2), Inches(12), Inches(0.5), "Three research questions", size=22, bold=True, color=BLUE)
bullets(s, Inches(0.6), Inches(3.8), Inches(12.1), Inches(2.8), [
    ("RQ1", "How do partitions, trigger interval and processing mode trade latency against throughput?"),
    ("RQ2", "Which settings prevent lost or duplicated data when a broker crashes, and how fast does Spark recover from its own crash?"),
    ("RQ3", "What does exactly-once output cost compared with at-least-once?"),
], size=20, gap=14)

# ---------------------------------------------------------------------------------------------- 3
s = new_slide(
    "One docker compose up starts the whole pipeline",
    "This is what we built, and we will come back to it. A seeded generator publishes trades, keyed by symbol, to a Kafka topic with six partitions on three brokers. Spark reads one task per partition, builds one-minute bars with a ten second watermark, and writes each batch to PostgreSQL in one transaction. Grafana reads PostgreSQL. The dashed line is Spark's checkpoint. Because the generator is seeded, we know the correct output in advance, so we can check every run.",
    "Arya", 40,
)
y = Inches(2.3)
box(s, Inches(0.6), y, Inches(2.2), Inches(1.5), ["Generator", "seeded, or live", "Coinbase feed"], size=14, bold=True)
box(s, Inches(3.45), y, Inches(2.9), Inches(1.5), ["Kafka, 3 brokers", "topic trades, 6 partitions", "RF 3, min ISR 2"], fill=RGBColor(0xDC, 0xEA, 0xFA), line=BLUE, size=14, bold=True)
box(s, Inches(7.0), y, Inches(2.5), Inches(1.5), ["Spark", "local[4], 1 s trigger", "watermark 10 s"], fill=RGBColor(0xDF, 0xF4, 0xEB), line=AQUA, size=14, bold=True)
box(s, Inches(10.15), y, Inches(1.35), Inches(1.5), ["Postgres", "bars, alerts"], size=13, bold=True)
box(s, Inches(11.8), y, Inches(1.0), Inches(1.5), ["Grafana"], size=13, bold=True)
box(s, Inches(7.0), Inches(4.65), Inches(2.5), Inches(0.9), ["Checkpoint", "offsets + state"], size=13)
arrow(s, Inches(2.8), y + Inches(0.75), Inches(3.45), y + Inches(0.75))
arrow(s, Inches(6.35), y + Inches(0.75), Inches(7.0), y + Inches(0.75))
arrow(s, Inches(9.5), y + Inches(0.75), Inches(10.15), y + Inches(0.75))
arrow(s, Inches(11.5), y + Inches(0.75), Inches(11.8), y + Inches(0.75))
arrow(s, Inches(8.25), y + Inches(1.5), Inches(8.25), Inches(4.65), dashed=True)
text(s, Inches(2.5), Inches(1.85), Inches(1.2), Inches(0.4), "key = symbol", size=11, color=INK2)
text(s, Inches(5.7), Inches(1.85), Inches(1.5), Inches(0.4), "1 task / partition", size=11, color=INK2)
text(s, Inches(8.4), Inches(3.95), Inches(2.2), Inches(0.4), "replayed after a crash", size=11, color=INK2)
bullets(s, Inches(0.6), Inches(5.85), Inches(12), Inches(0.9), [
    "Every run is checked bar by bar against an oracle that computes the exact expected answer.",
], size=18)

# ---------------------------------------------------------------------------------------------- 4
s = new_slide(
    "Kafka: durability comes from replication, speed from batched sequential writes",
    'Kafka stores events in partitions, append-only files that are written and read sequentially in batches, which is why it is fast. Order holds within a partition, so keying by symbol keeps each symbol in order. Each partition is copied to three brokers: one leader, and followers that form the in-sync replica set. Durability depends on the replication factor, on how many copies the producer waits for, and on the minimum in-sync replicas. We use three, acks equal to all, and a minimum of two, plus an idempotent producer, because a retry after a crash can otherwise write a trade twice. We saw that happen.',
    "Arya", 75,
)
cols = [
    ("Partitions", ["Append-only log per partition", "Ordered within a partition only", "Key = symbol keeps each symbol in order"], BLUE),
    ("Replication", ["Leader plus followers", "In-sync replicas (ISR)", "RF 3, min ISR 2, no unclean election"], AQUA),
    ("Producer", ["acks = 0, 1 or all", "Idempotent: no duplicates on retry", "We use acks=all + idempotence"], ORANGE),
]
for i, (head, lines, col) in enumerate(cols):
    x = Inches(0.6 + i * 4.15)
    hb = box(s, x, Inches(1.7), Inches(3.9), Inches(0.7), head, fill=col, line=col, color=WHITE, size=20, bold=True)
    box(s, x, Inches(2.4), Inches(3.9), Inches(2.5), lines, fill=PANEL, size=17)
bullets(s, Inches(0.6), Inches(5.3), Inches(12.2), Inches(1.4), [
    "KRaft: metadata in a Raft log on the brokers themselves, no ZooKeeper (Kafka 4.4).",
    "Consumer groups and offsets exist, but Spark does not use them. Why is on the integration slide.",
], size=18)

# ---------------------------------------------------------------------------------------------- 5
s = new_slide(
    "Spark: a stream is a table that never stops growing",
    'Thanks, Arya. Spark treats a stream as a table that keeps growing and runs a normal query over it, in micro-batches: a trigger starts a batch, and the batch reads the offsets that arrived since the last one. Our query builds one-minute event-time windows per symbol. A ten second watermark says how long to wait for late trades, and each bar is written once, when the watermark passes its minute. State and offsets go to a checkpoint. Continuous mode has lower latency, but it only handles per-record operations, so it cannot run our bars job; we measure it with a pass-through job.',
    "Rawad", 75,
)
rows = [
    ("Micro-batch", "A trigger starts a batch every interval; each batch reads the new offsets."),
    ("Event-time windows", "1-minute tumbling windows per symbol: open, high, low, close, VWAP."),
    ("Watermark (10 s)", "Late data is accepted for 10 s; a window is emitted once, when the watermark passes it."),
    ("State store + checkpoint", "Partial aggregates and each batch's offsets are saved after every batch."),
    ("Continuous mode", "Lower latency, but only per-record operations, at-least-once, one core per partition."),
]
for i, (a, b) in enumerate(rows):
    yy = Inches(1.65 + i * 1.02)
    box(s, Inches(0.6), yy, Inches(3.5), Inches(0.9), a, fill=RGBColor(0xDF, 0xF4, 0xEB), line=AQUA, size=17, bold=True)
    box(s, Inches(4.25), yy, Inches(8.5), Inches(0.9), b, fill=PANEL, size=16)

# ---------------------------------------------------------------------------------------------- 6
s = new_slide(
    "Exactly-once output = replay from the checkpoint + an idempotent sink",
    'Here the two systems meet. Each Kafka partition becomes one Spark task. Spark keeps offsets in its own checkpoint, written before a batch is processed, so offsets are tied to the state and the output; a consumer group offset only says consumed. After a crash Spark replays the unfinished batch, so the output must tolerate a second write. Each batch goes to PostgreSQL in one transaction with an upsert on symbol and window, so a replay changes nothing. A crash between steps five and six is exactly what our fourth experiment tests.',
    "Rawad", 75,
)
steps = [
    "1  Read the latest offsets",
    "2  Write the planned range to the checkpoint",
    "3  Read the range, update the state",
    "4  Emit windows the watermark has passed",
    "5  One transaction: upsert bars, upsert alerts, commit",
    "6  Write the commit record to the checkpoint",
]
for i, t in enumerate(steps):
    hot = i in (4, 5)
    box(s, Inches(0.6), Inches(1.6 + i * 0.82), Inches(7.2), Inches(0.7), t, fill=RGBColor(0xFD, 0xE8, 0xDF) if hot else PANEL,
        line=ORANGE if hot else LINE, size=17, bold=hot)
text(s, Inches(0.6), Inches(6.55), Inches(7.2), Inches(0.4), "A crash between 5 and 6 replays the batch (Experiment E4).", size=14, color=ORANGE, bold=True)
bullets(s, Inches(8.2), Inches(1.6), Inches(4.6), Inches(5), [
    ("Partitions become tasks.", "One Spark task per Kafka partition."),
    ("Offsets in the checkpoint,", "not in a consumer group: tied to state and output."),
    ("Replay is normal.", "So a second write must change nothing."),
    ("Upsert on (symbol, window)", "makes the replay harmless."),
], size=17, gap=14)

# ---------------------------------------------------------------------------------------------- 7
s = new_slide(
    "Correctness first: every run is compared with an exact answer",
    'A benchmark from a wrong run is worthless, so every run is checked. A seeded generator makes about sixty thousand trades with twenty price shocks at known times. An oracle computes the expected four thousand bars and twenty-three alerts. The verifier compares every bar and alert, and a second check compares what the producer was told was acknowledged with what is in the topic. Every experiment, crashes included, ends in pass or fail.',
    "Rawad", 55,
)
for i, (n, label) in enumerate([("60,163", "trades, 100 symbols"), ("4,000", "expected bars"), ("23", "expected alerts"), ("20 / 20", "injected shocks detected")]):
    x = Inches(0.6 + i * 3.1)
    box(s, x, Inches(1.7), Inches(2.9), Inches(1.7), [n, label], fill=PANEL, size=16, color=INK2)
    # make the number large
    tf = s.shapes[-1].text_frame
    tf.paragraphs[0].runs[0].font.size = Pt(36)
    tf.paragraphs[0].runs[0].font.bold = True
    tf.paragraphs[0].runs[0].font.color.rgb = BLUE
bullets(s, Inches(0.6), Inches(3.8), Inches(12.2), Inches(2.8), [
    ("Oracle:", "a single script defines the trade model, the bars and the alert rule; the verifier uses a copy, and a test fails if they differ."),
    ("Verifier:", "open, high, low, close exact; VWAP within 1e-6; every alert; no missing, extra or duplicate rows."),
    ("Kafka-layer check:", "acknowledged trades must be in the topic exactly once."),
    ("Result:", "make golden passes: 4,000 of 4,000 bars and 23 of 23 alerts, also after killing Spark and a broker."),
], size=18, gap=10)

# ---------------------------------------------------------------------------------------------- 8
s = new_slide(
    "Live demo: dashboard, an alert, a Spark crash, a broker crash, then PASS",
    'DEMO SCRIPT (about 2.5 minutes live). Start from a clean stack: make reset, then make up, and wait about a minute before recording.\n\n1. Dashboard (30 s). Open localhost:3000. Bars arrive for 100 symbols at thirty times speed. Pick a symbol: candlesticks, VWAP line, and the health row, lag zero and seconds since the last bar.\n2. An alert (25 s). After about eighty seconds the first alerts appear. Pick one in the alerts table, select that symbol, and show the price jump in its candlestick.\n3. Spark crash (35 s). Run: docker kill spark-bars, then docker compose up -d spark-bars. Seconds since the last bar climbs, then bars resume. Experiment three measured how long.\n4. Broker crash (35 s). Run: docker kill kafka-2. Bars keep arriving, because replication factor three with a minimum of two tolerates one broker. Bring it back with docker compose up -d kafka-2.\n5. Proof (25 s). Show the final PASS line of the verifier, or the output of scripts/live_check.sh with KILL_SPARK_AT=15 and KILL_BROKER_AT=35 run beforehand: every bar and alert equals the oracle despite both crashes.\n\nFallback: record the demo separately and splice it in. Live Coinbase data is shown later.',
    "Mark", 180,
)
picture(s, "dashboard-live.png", Inches(0.6), Inches(1.5), h=Inches(5.0))
bullets(s, Inches(7.5), Inches(1.6), Inches(5.3), Inches(5), [
    "1  Dashboard: 100 symbols, health row",
    "2  A price shock and its alert",
    "3  docker kill spark-bars: bars resume",
    "4  docker kill kafka-2: bars keep coming",
    "5  Verifier: PASS, identical to the oracle",
], size=19, gap=14)
source(s, "Screenshot: the dashboard during the live Coinbase run (10 products).")

# ---------------------------------------------------------------------------------------------- 9
s = new_slide(
    "Reproduction (a): batching multiplies producer throughput 26 times",
    "Our first reproduction is Figure 4 of the original Kafka paper: batching makes a producer much faster. We used Kafka's performance tool with ten million records of two hundred bytes and no acknowledgements. Throughput goes from about twenty thousand records per second unbatched to over five hundred thousand with batches of fifty, twenty-six times. The paper reports about eight times. Our absolute numbers differ because producer and broker share one four-core machine, but the shape is the same.",
    "Rawad", 60,
)
picture(s, "e1a_producer_batching.png", Inches(0.5), Inches(1.5), w=Inches(8.6))
bullets(s, Inches(9.3), Inches(1.7), Inches(3.6), Inches(4.5), [
    ("19,798 to 525,376", "records/s from batch size 1 to 50: 26 times."),
    ("The paper:", "about 8 times (50,000 to 400,000)."),
    ("Same shape.", "Different absolute numbers: one shared 4-core machine."),
], size=17, gap=14)
source(s, "Reproduces Kreps et al. 2011, Figure 4. Median of 3 runs; 10 million records of 200 bytes, acks=0.")

# ---------------------------------------------------------------------------------------------- 10
s = new_slide(
    "Reproduction (b): continuous mode answers in tens of milliseconds, until it saturates",
    'The second reproduction is Figure 7 of the Structured Streaming paper. We ran a stateless pass-through job in both modes. Continuous mode answers in forty-three to eighty-three milliseconds up to forty thousand records per second, then saturates at eighty thousand, where the median jumps to 1.8 seconds. Micro-batch keeps up with at least a hundred thousand, the most our generator can produce, at a few hundred milliseconds. Continuous mode trades throughput for latency, as the paper says. Their latency was about one millisecond; ours includes producer batching and a three-replica write.',
    "Rawad", 70,
)
picture(s, "e1b_continuous_vs_microbatch.png", Inches(0.5), Inches(1.5), w=Inches(8.6))
bullets(s, Inches(9.3), Inches(1.7), Inches(3.6), Inches(4.5), [
    ("Continuous:", "p50 43 to 83 ms up to 40,000 records/s; saturates at 80,000 (p50 1.8 s)."),
    ("Micro-batch:", "at least 100,000 records/s (our input limit), p50 0.3 to 0.6 s."),
    ("Limit:", "one core per partition, so 4 partitions on 4 cores."),
], size=17, gap=14)
source(s, "Reproduces Armbrust et al. 2018, Figure 7. Pass-through job, 4 partitions, local[4]. Median of 3 runs.")

# ---------------------------------------------------------------------------------------------- 11
s = new_slide(
    "RQ1: the best trigger depends on load; more partitions than cores do not help",
    'For the first question we ran the real bars job at thirty, sixty and ninety thousand records per second. One partition sustains about seventy-five thousand, and three or six sustain at least ninety thousand, the limit of our generator. On latency, the best trigger changes with load. At thirty thousand a half-second trigger is fastest, 1.7 seconds. At sixty thousand a batch takes about two seconds whatever the trigger, so a shorter trigger only adds overhead, and one second wins at 4.9 seconds. Five seconds is always slowest.',
    "Mark", 80,
)
picture(s, "e1c_sustained_throughput.png", Inches(0.45), Inches(1.5), w=Inches(6.3))
picture(s, "e1c_bar_latency_60k.png", Inches(6.85), Inches(1.5), w=Inches(6.0))
bullets(s, Inches(0.6), Inches(4.85), Inches(12.2), Inches(1.9), [
    ("Throughput:", "1 partition about 75,000 records/s; 3 and 6 partitions at least 90,000 (our input limit)."),
    ("Latency:", "500 ms trigger best at 30,000 records/s (1.7 s); 1 s best at 60,000 (4.9 s); 5 s always slowest."),
], size=17, gap=8)
source(s, "Bars job, local[4]. The sweeps ran on different CPUs, so values are compared within one sweep only.")

# ---------------------------------------------------------------------------------------------- 12
s = new_slide(
    "RQ2: no acknowledged trade lost in 12 broker kills, but acks=all stalls for 9 seconds",
    "Second question: a broker dies. We killed the leader of partition zero twelve times, and no acknowledged trade was lost. With acks equal to all, writes stalled for about nine seconds on every partition, because the dead broker stays in the in-sync set until it is fenced, which takes the nine second session timeout. With acks equal to one, only the dead broker's partitions stalled, but that setting can lose acknowledged writes; we saw it once. In one run Spark dropped trades as late, because partitions resumed 3.8 seconds apart and our time compression stretched that past the watermark. The watermark must exceed the failover skew.",
    "Mark", 85,
)
picture(s, "e2_broker_crash.png", Inches(0.5), Inches(1.5), w=Inches(8.6))
bullets(s, Inches(9.3), Inches(1.7), Inches(3.6), Inches(4.8), [
    ("0 lost", "in 12 leader kills (RF 3, min ISR 2)."),
    ("acks=all:", "every partition stalls about 9 s: the session timeout."),
    ("acks=1:", "only the dead leader's partitions stall, but acknowledged writes can be lost."),
    ("Watermark", "must exceed failover skew or Spark drops late trades."),
], size=16, gap=12)
source(s, "Kill of the partition-0 leader at 50% of a 40-minute virtual run. 12 kills: 3 acks=all, 6 acks=1, 3 without idempotence.")

# ---------------------------------------------------------------------------------------------- 13
s = new_slide(
    "RQ2: Spark restarts from its checkpoint in 13 to 15 seconds, with exact output",
    'We also killed Spark with SIGKILL and restarted it on the same checkpoint. The first new bar appeared 12.8 seconds later in replay and 14.6 seconds later on a live stream, and the output was exact in all six runs. Almost all of that time is JVM start-up. The checkpoint plus the upsert sink make the replay safe.',
    "Rawad", 40,
)
picture(s, "e3_spark_recovery.png", Inches(0.6), Inches(1.5), w=Inches(7.6))
bullets(s, Inches(8.6), Inches(1.8), Inches(4.3), Inches(4), [
    ("12.8 s", "kill to first new bar, deterministic replay."),
    ("14.6 s", "on a live stream."),
    ("6 of 6", "runs produced exact output."),
    ("Mostly start-up:", "JVM and Spark, not recovery logic."),
], size=18, gap=14)
source(s, "SIGKILL to the Spark container, restart on the same checkpoint volume. Median of 3 runs per mode.")

# ---------------------------------------------------------------------------------------------- 14
s = new_slide(
    "RQ3: plain inserts leave 300 duplicate rows after a crash; upserts leave none, at no measurable cost",
    'Third question: what does exactly-once cost. We made Spark kill itself after the database commit and before it recorded the batch, so the restart replays a batch that is already in the database. Plain inserts left three hundred duplicate rows every time. Upserts left none. Without a crash, at two thousand records per second, batch duration was 460 milliseconds for inserts and 451 for upserts, so we measured no cost, with the caveat that our database is local.',
    "Mark", 60,
)
picture(s, "e4_exactly_once.png", Inches(0.5), Inches(1.5), w=Inches(8.6))
bullets(s, Inches(9.3), Inches(1.7), Inches(3.6), Inches(4.5), [
    ("300 vs 0", "duplicate rows after the crash, in every run."),
    ("460 vs 451 ms", "batch duration, insert vs upsert."),
    ("Latency", "ranges overlap: no measurable cost at 2,000 records/s."),
], size=17, gap=14)
source(s, "Crash between the database commit and Spark's commit. 3 runs per setting. Cost runs: crash-free, 2,000 records/s.")

# ---------------------------------------------------------------------------------------------- 15
s = new_slide(
    "Challenges and limits: what one machine can and cannot show",
    "Things went wrong along the way: an unpaced replay made small skews look like late data, a retry duplicated a trade until we enabled idempotence, continuous mode silently skipped two of six partitions on four cores, and a SIGKILL to a container's main process is ignored without an init process. The instructors asked us to discuss the limits of one machine. Everything shared four CPUs and one disk, so our rates are this machine's capacity, and above ninety thousand they are our generator's. Brokers talk over loopback, so acks equal to all looks cheaper than on a network. A killed container is not a failed node, so the nine second stall reflects our timeout setting. What holds is correctness: exact output, nothing acknowledged lost, and the relative comparisons.",
    "Arya", 90,
)
text(s, Inches(0.6), Inches(1.5), Inches(5.9), Inches(0.5), "Challenges we hit", size=22, bold=True, color=BLUE)
bullets(s, Inches(0.6), Inches(2.1), Inches(5.9), Inches(4.5), [
    "Unpaced replay made skew look like late data",
    "Producer retry duplicated a trade until idempotence was on",
    "Continuous mode silently skipped 2 of 6 partitions on 4 cores",
    "SIGKILL to PID 1 is ignored: needed an init process",
    "Windows ports and paths broke our first live run",
], size=17, gap=10)
text(s, Inches(6.9), Inches(1.5), Inches(5.9), Inches(0.5), "Limits of one 4-CPU machine", size=22, bold=True, color=ORANGE)
bullets(s, Inches(6.9), Inches(2.1), Inches(5.9), Inches(4.5), [
    "Rates are this machine's capacity; above 90,000 records/s, our generator's",
    "Loopback network: acks=all latency understated",
    "A killed container is not a failed node or a partition",
    "9 s stall = the session timeout setting",
    "Still true: output exact, nothing acknowledged lost, relative comparisons",
], size=17, gap=10)

# ---------------------------------------------------------------------------------------------- 16
s = new_slide(
    "It also works on real trades: 230 of 230 live bars matched",
    "Finally, real data. We ran the producer against Coinbase's public feed for ten products for more than twenty minutes. The dashboard filled with bars for all ten symbols with Spark's lag at zero. No alert fired, which is expected, because real prices rarely move three standard deviations in that time. The verifier compared the output with the topic itself, and all 230 bars matched. A capture and replay mode lets the demo run offline.",
    "Mark", 50,
)
picture(s, "dashboard-live.png", Inches(0.6), Inches(1.5), h=Inches(5.0))
bullets(s, Inches(7.5), Inches(1.8), Inches(5.3), Inches(4.5), [
    ("10 products,", "20+ minutes of live Coinbase trades."),
    ("230 of 230 bars", "matched the topic's contents."),
    ("No alerts:", "expected on real prices."),
    ("Offline replay", "from a capture for the demo."),
], size=18, gap=14)

# ---------------------------------------------------------------------------------------------- 17
s = new_slide(
    "Conclusions: speed, durability and exactly-once hold, if configured and measured",
    'To sum up. Processing mode sets the latency floor, and the trigger should match the load. With replication factor three and a minimum of two, no acknowledged trade was lost, and Spark recovers in about fifteen seconds with exact output. Exactly-once output needs an idempotent sink, and it cost us nothing we could measure. Everything is reproducible: one docker compose up, make golden to check against the oracle, scripted experiments, and the code and data in the repository and the zip. Thank you. We are happy to take questions.',
    "All three", 60,
)
for i, (rq, line, col) in enumerate([
    ("RQ1", "Mode sets the latency floor; match the trigger to the load; partitions beyond cores do not help.", BLUE),
    ("RQ2", "RF 3 + min ISR 2: 0 acknowledged trades lost; Spark back in about 15 s, exact output.", AQUA),
    ("RQ3", "Exactly-once needs an idempotent sink: 300 duplicates vs 0, no measurable cost.", ORANGE),
]):
    yy = Inches(1.6 + i * 1.2)
    box(s, Inches(0.6), yy, Inches(1.3), Inches(1.0), rq, fill=col, line=col, color=WHITE, size=22, bold=True)
    box(s, Inches(2.05), yy, Inches(10.7), Inches(1.0), line, fill=PANEL, size=18)
text(s, Inches(0.6), Inches(5.35), Inches(12), Inches(0.45), "Reproduce it", size=22, bold=True, color=BLUE)
bullets(s, Inches(0.6), Inches(5.85), Inches(12.2), Inches(1.0), [
    "docker compose up starts everything  |  make golden checks it against the oracle  |  make bench-all reruns the experiments",
], size=16)

prs.save(OUT)

# Plain-text speaker script (the same text as the slide notes), for rehearsal and printing.
lines = ["SPEAKER SCRIPT: Group 6, 15-minute presentation", ""]
for n, sl in enumerate(prs.slides, 1):
    lines += [f"--- Slide {n} ---", sl.notes_slide.notes_text_frame.text, ""]
(OUT.parent / "speaker-script.txt").write_text("\n".join(lines))
print(f"wrote {OUT.relative_to(ROOT)}: {len(prs.slides)} slides")
