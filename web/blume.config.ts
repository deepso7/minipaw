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
    actions: [
      { href: "/docs", label: "Docs" },
      { href: "/changelog", label: "Changelog" },
      { href: "https://github.com/deepso7/minipaw", label: "GitHub" },
    ],
  },
  // Colors beyond accent and background live in theme.css.
  theme: {
    accent: {
      dark: "oklch(82.2% 0.076 31.9)",
      light: "oklch(55.5% 0.106 34.1)",
    },
    background: {
      dark: "oklch(20.3% 0.008 297.1)",
      light: "oklch(97.4% 0.003 308.4)",
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
