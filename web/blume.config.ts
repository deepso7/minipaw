import { defineConfig } from "blume";
import { cloudflare } from "blume/deploy";
import { filesystem, githubReleases } from "blume/sources";

// https://paper.design/mono (OFL, see fonts/PaperMono-OFL.txt), for every role.
const paperMono = {
  name: "Paper Mono",
  variants: [{ src: "./fonts/PaperMono-Variable.woff2" }],
};

export default defineConfig({
  title: "minipaw",
  description:
    "Pipe stdin/stdout between two machines, peer to peer. Like netcat, but it gets through NATs, needs no accounts, and every byte is end-to-end encrypted.",
  logo: "/logo.svg",
  deployment: cloudflare({ output: "static", site: "https://paw.minip2p.com" }),
  github: {
    owner: "deepso7",
    repo: "minipaw",
    dir: "web",
  },
  navigation: {
    actions: [
      { href: "/docs", label: "Docs" },
      { href: "/changelog", label: "Changelog" },
      { href: "https://github.com/deepso7/minipaw", label: "GitHub" },
    ],
  },
  content: {
    sources: [
      filesystem({ root: "md" }),
      // Changelog entries from GitHub Releases. Private repos read
      // GITHUB_TOKEN from the environment.
      githubReleases({
        owner: "deepso7",
        repo: "minipaw",
        prefix: "changelog",
      }),
    ],
  },
  // Colors beyond accent and background live in theme.css.
  theme: {
    accent: {
      light: "oklch(55.5% 0.106 34.1)",
      dark: "oklch(82.2% 0.076 31.9)",
    },
    background: {
      light: "oklch(97.4% 0.003 308.4)",
      dark: "oklch(20.3% 0.008 297.1)",
    },
    fonts: {
      display: paperMono,
      body: paperMono,
      mono: paperMono,
    },
  },
});
