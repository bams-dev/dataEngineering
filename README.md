# Évaluation Spark — DataFrame API Thèmes mixtes
## Master 2 Big Data

| | |
|---|---|
| **Donné** | Samedi |
| **Rendu** | Mardi avant 8h00 — `git push` |
| **Barème** | 20 pts — 4 exercices × 5 pts (code + théorie + théorie avancée) |

---

##  Démarrage

### 1. Copiez votre dataset personnel
```bash
cp /chemin/votre_dataset/* src/main/resources/
```
*(4 fichiers reçus individuellement : ventes.csv, clients.csv, produits.csv, retours.csv)*

### 2. Ouvrez dans IntelliJ IDEA
- `File → Open` → ce dossier → **"Load sbt Project"**
- Attendez ~3-5 min le téléchargement des dépendances

### 3. Lancez
```bash
sbt run
# ou : clic droit sur EvaluationSpark.scala → Run
```

---

##  Structure

```
evaluation-spark-sbt/
├── build.sbt
├── REPONSES.md          ← Questions théoriques  à compléter
├── README.md
└── src/main/
    ├── scala/evaluation/
    │   └── EvaluationSpark.scala   ← CODE à compléter (12 méthodes)
    └── resources/
        ├── ventes.csv              ← Votre dataset personnel
        ├── clients.csv
        ├── produits.csv
        └── retours.csv
```

---

##  Ce que vous devez rendre

| Fichier | Contenu |
|---|---|
| `EvaluationSpark.scala` | 12 méthodes complétées (plus de `???`) |
| `REPONSES.md` | 5 questions théoriques  |

```bash
git add .
git commit -m "Rendu - [Votre Nom]"
git push origin main
```

---

## 💡 Rappels importants

- Spark en mode **`local[*]`** — aucun cluster requis
- Les `???` non remplacés = **0 pt** pour l'exercice concerné
- Commitez **régulièrement**, pas uniquement le mardi matin
- En cas de `OutOfMemoryError` : créez `.jvmopts` avec `-Xmx4g`
