"""Échantillonne la mémoire utilisée et le CPU de la machine. Usage :
   python monitor.py fichier.csv &        (enregistre jusqu'à kill)
   python monitor.py --resume fichier.csv (affiche le résumé)"""
import csv, sys, time
import psutil

if sys.argv[1] == "--resume":
    rows = list(csv.DictReader(open(sys.argv[2])))
    mem = [float(r["mem_used_gb"]) for r in rows]
    cpu = [float(r["cpu_pct"]) for r in rows]
    duree = float(rows[-1]["t"]) - float(rows[0]["t"])
    print(f"durée {duree:.0f} s | pic RAM {max(mem):.1f} Go (base {mem[0]:.1f}) | CPU moyen {sum(cpu)/len(cpu):.0f} %")
else:
    with open(sys.argv[1], "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["t", "mem_used_gb", "cpu_pct"])
        while True:
            w.writerow([time.time(), psutil.virtual_memory().used / 1e9, psutil.cpu_percent(interval=0.5)])
            fh.flush()
