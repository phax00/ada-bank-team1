# Horizon AI bank mobile demo

Klikatelne demo mobilni aplikace pro use case:

1. klient odejde na `edalnice.cz`,
2. na `edalnice.cz` zaplati dalnicni znamku pres Apple Pay, Google Pay nebo kartu,
3. Horizon AI bank rozpozna transakci jako vehicle signal a kratce vyhodnoti propenzitni model,
4. banka posle push notifikaci,
5. klient klikne na push a otevre se mu personalizovana nabidka pojisteni vozidel v appce.

## Spusteni

Slozka dema:

```text
SEM/Horizon_AI_Bank_Mobile_Demo
```

Varianta 1:
- otevrit `index.html` v prohlizeci

Varianta 2:
- v teto slozce spustit:

```powershell
python -m http.server 8080
```

- a otevrit:

```text
http://localhost:8080
```

## Obsah

- `index.html` - struktura mobilniho dema
- `styles.css` - light/dark motiv a mobilni layout
- `app.js` - klikatelny flow a simulace push notifikace

## Poznamka k brandu

V demu je pouzit nazev `Horizon AI bank`. `AI` je stylizovano jako exponent u logotypu.
