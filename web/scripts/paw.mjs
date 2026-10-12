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

// One <path> per colour, built from horizontal runs of same-coloured pixels.
// `square` centres the art on a square canvas, so favicons aren't stretched.
const svg = (grid, { square = false, className } = {}) => {
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
    .map(([c, d]) => `<path fill="${palette[c]}" d="${d.join("")}"/>`)
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
