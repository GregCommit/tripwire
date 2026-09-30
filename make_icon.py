"""Draw Tripwire's home-screen icon: an amber lightning bolt snapping a thin trip wire,
on the app's dark background. Drawn large and scaled down for smooth edges.

  python make_icon.py     writes static/icon-180.png, static/icon-512.png
"""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFilter

S = 1024
AMBER, AMBER_DEEP, BG_TOP, BG_BOTTOM = (245, 158, 11), (217, 119, 6), (30, 34, 53), (10, 12, 18)


def draw():
    img = Image.new("RGB", (S, S))
    top, bot = BG_TOP, BG_BOTTOM
    grad = ImageDraw.Draw(img)
    for y in range(S):                                    # soft vertical gradient
        t = y / (S - 1)
        grad.line([(0, y), (S, y)], fill=tuple(round(a + (b - a) * t) for a, b in zip(top, bot)))
    # The trip wire: a thin line across, broken where the bolt passes through.
    wire = Image.new("L", (S, S), 0)
    w = ImageDraw.Draw(wire)
    y = int(S * 0.64)
    w.line([(int(S * 0.08), y), (int(S * 0.29), y)], fill=255, width=22)
    w.line([(int(S * 0.67), y), (int(S * 0.92), y)], fill=255, width=22)
    for x in (int(S * 0.08), int(S * 0.92)):              # the wire's two anchor posts
        w.ellipse([x - 26, y - 26, x + 26, y + 26], fill=255)
    img.paste(Image.new("RGB", (S, S), (156, 163, 175)), mask=wire)
    # The bolt, with a warm glow behind it.
    bolt = [(0.58, 0.10), (0.30, 0.56), (0.47, 0.56), (0.40, 0.90), (0.72, 0.42), (0.54, 0.42), (0.64, 0.10)]
    pts = [(x * S, y * S) for x, y in bolt]
    glow = Image.new("L", (S, S), 0)
    ImageDraw.Draw(glow).polygon(pts, fill=150)
    glow = glow.filter(ImageFilter.GaussianBlur(40))
    img.paste(Image.new("RGB", (S, S), AMBER_DEEP), mask=glow)
    shape = Image.new("L", (S, S), 0)
    ImageDraw.Draw(shape).polygon(pts, fill=255)
    fill = Image.new("RGB", (S, S))
    fd = ImageDraw.Draw(fill)
    for yy in range(S):                                   # bolt lighter at the top
        t = yy / (S - 1)
        fd.line([(0, yy), (S, yy)], fill=tuple(round(a + (b - a) * t) for a, b in zip((252, 211, 77), AMBER)))
    img.paste(fill, mask=shape)
    return img


if __name__ == "__main__":
    out = Path(__file__).resolve().parent / "static"
    out.mkdir(exist_ok=True)
    big = draw()
    for size in (180, 512):
        big.resize((size, size), Image.LANCZOS).save(out / f"icon-{size}.png", optimize=True)
    print("wrote", [p.name for p in sorted(out.glob("icon-*.png"))])
