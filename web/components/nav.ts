import type { NavNode } from "blume";

// The docs index (/docs, "What minipaw is") lists first under Getting
// started, as Executor's docs do. Blume can't place it there itself: a
// folder's own index becomes the group's header link rather than a row in
// it, and an explicit sidebar is lost to the Docs tab's folder scoping. So
// the sidebar moves the node over before drawing the tree. It returns new
// arrays and objects instead of editing them, since Blume shares one tree
// between pages.
const INDEX = "/docs";
const GROUP = "Getting started";

export const withIndexInGettingStarted = (items: NavNode[]): NavNode[] => {
  const index = items.find((node) => node.kind === "page" && node.route === INDEX);
  const group = items.find((node) => node.kind === "group" && node.label === GROUP);
  if (!index || !group || group.kind !== "group") {return items;}
  return items
    .filter((node) => node !== index)
    .map((node) => (node === group ? { ...group, children: [index, ...group.children] } : node));
};
