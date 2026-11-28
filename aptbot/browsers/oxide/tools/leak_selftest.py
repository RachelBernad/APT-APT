from browser_oxide import Browser, Profile
import os
with Browser(profile=Profile.chrome()) as b:
    b.navigate("https://example.com/", max_iterations=3)
    def run(label, body):
        h1 = int(b.evaluate("#bo:gc"))
        for _ in range(20):
            b.evaluate("for (let i = 0; i < 1000; i++) { " + body + " } 1"); b.pump(10)
        h2 = int(b.evaluate("#bo:gc"))
        print(f"{label:38s} +{(h2-h1)/1e6:5.1f} MB", flush=True)
    run("create only (never attached)", "const d = document.createElement('div');")
    run("attach+remove + innerHTML span", "const d = document.createElement('div'); d.innerHTML = '<span>' + 'x'.repeat(200) + '</span>'; document.body.appendChild(d); d.remove();")
    run("attach+remove + listener(big closure)", "const d = document.createElement('div'); document.body.appendChild(d); const big = new Array(50).fill(i); d.addEventListener('click', () => big.length); d.remove();")
    run("detached + listener(big closure)", "const d = document.createElement('div'); const big = new Array(50).fill(i); d.addEventListener('click', () => big.length);")
    run("attached (kept) + listener", "const d = document.createElement('div'); document.body.appendChild(d); d.addEventListener('click', () => i);")
    print(b.evaluate("(() => { let n = 0; document.body.querySelectorAll('div').forEach(d => d.dispatchEvent(new Event('click'))); return 'still-attached listeners ok: ' + document.body.querySelectorAll('div').length; })()"), flush=True)
    os._exit(0)
