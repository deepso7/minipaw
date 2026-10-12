import { defineConfig } from "oxfmt";
import ultracite from "ultracite/oxfmt";

export default defineConfig({
  ...ultracite,
  // "never" joins every paragraph onto one line, which folds Blume's
  // :::callout blocks into plain text.
  proseWrap: "preserve",
});
