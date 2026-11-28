#!/bin/sh
# usage: ab.sh LABEL "V8 FLAGS"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "${OXIDE_WORK:-$HOME/aptbot-browser-research/browseroxide}"
cp profiles/fbtest_jar.json /tmp/fbjar_run.json && chmod 600 /tmp/fbjar_run.json
TARGET_MODE=opened BROWSER_OXIDE_HEAP_INITIAL_MB=16 BROWSER_OXIDE_HEAP_MAX_MB=384 BROWSER_OXIDE_V8_FLAGS="$2" BROWSER_OXIDE_COOKIE_JAR=/tmp/fbjar_run.json BO_MEM_MB=1500 BO_TIMEOUT=600 venv/bin/python "$HERE/tools/guard.py" venv/bin/python "$HERE/research/fb_deep_session.py" https://www.facebook.com/groups/240748264581079/ 6 out/deep/ab_$1.csv 2>/dev/null | grep -a "DONE"
python3 -c "
import csv,statistics as st; r=[int(x['rss_mb']) for x in csv.DictReader(open('out/deep/ab_$1.csv'))]
print('$1', 'mean', round(st.mean(r)), 'p90', sorted(r)[int(len(r)*.9)], 'max', max(r), 'samples', len(r))"
