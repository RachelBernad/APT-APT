"""Structural census of a page from a saved HTML snapshot, without class names (playbook §7).

    python -m aptbot.scrapers.facebook_generic_browser.explore.tools.domcensus SNAPSHOT.html [--container 'role=feed']

Prints role/aria landmark counts for the whole page, then describes each direct child of the container
(default: the first [role=feed]): text length, images, roles, aria/data attributes, interesting links.
Reveals virtualization (children with no text but a data-virtualized/aria-labelledby marker), skeletons, and
which attributes are stable enough to select on.
"""
import collections
import re
import sys
from html.parser import HTMLParser

VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track",
        "wbr", "path", "circle", "rect", "use", "line", "polyline", "polygon", "ellipse", "stop"}


class Census(HTMLParser):
    def __init__(self, attr, value):
        super().__init__()
        self.attr, self.value = attr, value
        self.stack, self.in_c, self.children, self.cur = [], None, [], None
        self.roles = collections.Counter()

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if a.get("role"):
            self.roles[a["role"]] += 1
        if tag in VOID:
            if self.cur is not None and tag == "img":
                self.cur["imgs"] += 1
            return
        depth = len(self.stack)
        self.stack.append(tag)
        if self.in_c is None and a.get(self.attr) == self.value:
            self.in_c = depth
            return
        if self.in_c not in (None, -1):
            if depth == self.in_c + 1:
                self.cur = {"attrs": sorted(k for k in a if k != "class"), "roles": collections.Counter(),
                            "aria": set(), "links": [], "imgs": 0, "text": 0}
                self.children.append(self.cur)
            elif self.cur is not None:
                if a.get("role"):
                    self.cur["roles"][a["role"]] += 1
                self.cur["aria"].update(k for k in a if k.startswith(("aria-", "data-")))
                if a.get("href"):
                    self.cur["links"].append(re.sub(r"[?&]__cft__.*", "", a["href"])[:80])

    def handle_endtag(self, tag):
        if tag in VOID:
            return
        while self.stack and self.stack.pop() != tag:
            pass
        if self.in_c not in (None, -1) and len(self.stack) <= self.in_c:
            self.in_c = -1

    def handle_data(self, d):
        if self.cur is not None and self.in_c not in (None, -1):
            self.cur["text"] += len(d.strip())


def main(argv):
    attr, value = "role", "feed"
    if "--container" in argv:
        attr, value = argv[argv.index("--container") + 1].split("=", 1)
    p = Census(attr, value)
    p.feed(open(argv[0]).read())
    print("page roles:", dict(p.roles.most_common(20)))
    print(f"container [{attr}={value}] children: {len(p.children)}")
    for i, c in enumerate(p.children):
        print(f"#{i:<3} text={c['text']:<6} imgs={c['imgs']:<3} attrs={c['attrs']} roles={dict(c['roles'])}")
        print(f"      aria/data={sorted(c['aria'])[:12]} links={c['links'][:4]}")


if __name__ == "__main__":
    main(sys.argv[1:])
