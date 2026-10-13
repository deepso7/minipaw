import { defineConfig } from "blume";
import { cloudflare } from "blume/deploy";
import { filesystem, githubReleases } from "blume/sources";

// https://paper.design/mono (OFL, see fonts/PaperMono-OFL.txt), for body and mono.
const paperMono = {
  name: "Paper Mono",
  variants: [{ src: "./fonts/PaperMono-Variable.woff2" }],
};

export default defineConfig({
  content: {
    sources: [
      filesystem({ root: "md" }),
      // Changelog entries from GitHub Releases. Private repos read
      // GITHUB_TOKEN from the environment.
      githubReleases({
        owner: "deepso7",
        prefix: "changelog",
        repo: "minipaw",
      }),
    ],
  },
  deployment: cloudflare({ output: "static", site: "https://paw.minip2p.com" }),
  description:
    "Pipe stdin/stdout between two machines, peer to peer. Like netcat, but it gets through NATs, needs no accounts, and every byte is end-to-end encrypted.",
  footer: {
    socials: {
      github: "https://github.com/deepso7/minipaw",
      x: "https://x.com/deepso7",
    },
  },
  github: {
    dir: "web",
    owner: "deepso7",
    repo: "minipaw",
  },
  logo: "/logo.svg",
  navigation: {
    actions: [{ href: "https://github.com/deepso7/minipaw", label: "GitHub" }],
    // Tabs, not header actions: a tab owns its section, so the release
    // entries under /changelog stay out of the docs sidebar while the
    // changelog index, feed and search still list them.
    tabs: [
      { label: "Docs", path: "/docs" },
      { label: "Changelog", path: "/changelog" },
    ],
  },
  seo: {
    og: {
      // Paper Mono only: the card tracks headlines at a fixed -0.05em, which
      // runs the pixel face's words together.
      fonts: [{ name: paperMono.name, src: paperMono.variants[0].src }],
      // The paw with hex fills (`pnpm paw`); the card's SVG renderer paints
      // oklch() black.
      logo: "./og/paw.svg",
      // The dark theme, from theme.css.
      palette: {
        accent: "#f09bb0",
        background: "#0e0d10",
        border: "#2a2730",
        foreground: "#dcd6c8",
        muted: "#8a8390",
      },
    },
  },
  // Colors beyond accent and background live in theme.css.
  theme: {
    // Toe-bean pink on a near-black instrument panel (dark), or a deeper
    // pink on cool paper (light).
    accent: {
      dark: "#f09bb0",
      light: "#b8325b",
    },
    background: {
      dark: "#0e0d10",
      light: "#f5f4f8",
    },
    fonts: {
      body: paperMono,
      // The pixel face for the wordmark and landing headings. It sits in the
      // display role only so Blume preloads it; theme.css points headings
      // back at Paper Mono.
      display: {
        name: "Pixelify Sans",
        variants: [{ src: "./fonts/pixelify-sans-600.woff2", weight: 600 }],
      },
      mono: paperMono,
    },
  },
  title: "minipaw",
});
