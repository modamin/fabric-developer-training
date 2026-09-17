# Notes pour l'instructeur — laboratoire d'ingestion Bronze

**Ce document n'est pas destiné aux étudiants.** Les étudiants suivent uniquement `bronze-ingestion.md`.

Le laboratoire exécute le pipeline trois fois pour montrer le chargement incrémentiel. Cela ne fonctionne que si les trois derniers mois sont absents du dépôt au début du cours, puis réapparaissent en cours de session.

`nb_list_event_files` répertorie les fichiers via l'**API GitHub Contents**; les deux actions ci-dessous doivent donc être **validées et poussées** — déplacer les fichiers dans un clone local n'a aucun effet pour les étudiants.

## Action 1 — avant la session : mettre de côté les trois derniers mois

```powershell
cd <path-to-repo>\data
Move-Item events\workforce_events_2025-1*.csv _parked_events\
git add -A
git commit -m "Park 2025-10..12 for the incremental lab"
git push
```

`data/events` contient alors 57 fichiers, le dernier étant `workforce_events_2025-09.csv`.

## Action 2 — après l'exécution initiale de tous les participants : les restaurer

Exécutez cette procédure une fois que toute la salle a terminé l'**exécution initiale** (étape 6.1), et avant que quiconque ne commence l'**exécution incrémentielle** (étape 6.2).

```powershell
cd <path-to-repo>\data
Move-Item _parked_events\workforce_events_2025-1*.csv events\
git add -A
git commit -m "Release the final three months"
git push
```

Les 60 fichiers sont de nouveau publiés; l'exécution incrémentielle copie donc exactement trois fichiers et l'exécution sans opération ne trouve rien de nouveau.

## Notes

- `2025-1*` correspond uniquement à `2025-10`, `2025-11` et `2025-12`; `2025-01` à `2025-09` contiennent un `0` et restent à leur emplacement.
- Laissez le dépôt restauré après le cours (60 fichiers dans `data/events`, `data/_parked_events` vide) afin que la session suivante puisse commencer par l'action 1.
