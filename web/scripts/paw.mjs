// Generates the pixel-art paw SVGs in public/ from the grids below.
// Edit a grid or the palette, then run `pnpm paw`.
import { writeFileSync } from "node:fs";

// Legend: o outline, f fur, d fur shade, p pad, h pad highlight, s pad shade.
const palette = {
  d: "oklch(79.3% 0.014 314.7)",
  f: "oklch(87.3% 0.011 316.5)",
  h: "oklch(91.5% 0.044 33.5)",
  o: "oklch(62.6% 0.018 313.5)",
  p: "oklch(82.2% 0.076 31.9)",
  s: "oklch(72.4% 0.092 34.7)",
};

// Paw print: header logo and favicon.
const print = [
  "....ooo..ooo....",
  "...offfoofffo...",
  "...ofhpffhpfo...",
  "..oofppffppfoo..",
  "offffssffssffffo",
  "ofhpffffffffhpfo",
  "ofppffhpppffppfo",
  "ofssfppppppfssfo",
  "offffppppppffffo",
  ".offppppppppffo.",
  "..ofsssffsssfo..",
  "...oddddddddo...",
  "....oooooooo....",
];

// Raised paw: landing page hero.
const raised = [
  ".....oooo..oooo.....",
  "....offffooffffo....",
  "...offhpffffhpffo...",
  "...ofphppffphppfo...",
  "...ofppppffppppfo...",
  "...offssffffssffo...",
  ".ooffffffffffffffoo.",
  "offhffffffffffffhffo",
  "ofpppfffhpppfffpppfo",
  "ofpppffphppppffpppfo",
  "offsffppppppppffsffo",
  "offfffppppppppfffffo",
  ".offffppppppppffffo.",
  "..offfsppppppsfffo..",
  "...offfssffssfffo...",
  "...offffffffffffo...",
  "...offffffffffffo...",
  "...oddddddddddddo...",
  "...oddddddddddddo...",
];

// Linear sRGB channel to two hex digits.
const channel = (v) => {
  const x = Math.min(1, Math.max(0, v));
  const srgb = x <= 0.0031308 ? 12.92 * x : 1.055 * x ** (1 / 2.4) - 0.055;

  return Math.round(srgb * 255)
    .toString(16)
    .padStart(2, "0");
};

// "oklch(L% C h)" to "#rrggbb". The OG card's SVG renderer paints oklch()
// fills black, so its copy of the logo is written in hex.
const hex = (color) => {
  const [l, c, h] = color.match(/[\d.]+/gu).map(Number);
  const hr = (h * Math.PI) / 180;
  const [L, a, b] = [l / 100, c * Math.cos(hr), c * Math.sin(hr)];

  const lms = [
    L + 0.3963377774 * a + 0.2158037573 * b,
    L - 0.1055613458 * a - 0.0638541728 * b,
    L - 0.0894841775 * a - 1.291485548 * b,
  ].map((v) => v ** 3);

  const rgb = [
    [4.0767416621, -3.3077115913, 0.2309699292],
    [-1.2684380046, 2.6097574011, -0.3413193965],
    [-0.0041960863, -0.7034186147, 1.707614701],
  ].map((row) => row.reduce((sum, k, i) => sum + k * lms[i], 0));

  return `#${rgb.map(channel).join("")}`;
};

// One <path> per colour, built from horizontal runs of same-coloured pixels.
// `square` centres the art on a square canvas, so favicons aren't stretched.
const svg = (grid, { square = false, className, fill = (c) => c } = {}) => {
  const runs = {};

  for (const [y, row] of grid.entries()) {
    for (let x = 0; x < row.length;) {
      const c = row[x];
      let end = x;

      while (row[end + 1] === c) {
        end += 1;
      }

      if (c !== ".") {
        (runs[c] ??= []).push(`M${x} ${y}h${end - x + 1}v1h-${end - x + 1}z`);
      }

      x = end + 1;
    }
  }

  let [x0, y0, w, h] = [0, 0, grid[0].length, grid.length];

  if (square) {
    const size = Math.max(w, h);
    [x0, y0, w, h] = [-(size - w) / 2, -(size - h) / 2, size, size];
  }

  const cls = className ? ` class="${className}"` : "";

  const paths = Object.entries(runs)
    .map(([c, d]) => `<path fill="${fill(palette[c])}" d="${d.join("")}"/>`)
    .join("");

  return `<svg xmlns="http://www.w3.org/2000/svg"${cls} viewBox="${x0} ${y0} ${w} ${h}" width="${w}" height="${h}" shape-rendering="crispEdges">${paths}</svg>\n`;
};

const out = new URL("../public/", import.meta.url);

writeFileSync(
  new URL("logo.svg", out),
  svg(print, { className: "minipaw-logo" })
);

writeFileSync(new URL("icon.svg", out), svg(print, { square: true }));

writeFileSync(new URL("paw-raised.svg", out), svg(raised));

// The generated OG cards' brand mark (seo.og.logo). Square, so the card's
// 32px mark puts each sprite pixel on 2 image pixels.
writeFileSync(
  new URL("../og/paw.svg", import.meta.url),
  svg(print, { fill: hex, square: true })
);
