import { defineMeta } from "blume";

export default defineMeta({
  // Group folders (in parentheses) add sidebar sections without a URL
  // segment, so /docs/usage stays /docs/usage.
  pages: ["getting-started", "ssh-access", "under-the-hood"],
  title: "Docs",
});
