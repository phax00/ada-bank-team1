const body = document.body;
const screens = [...document.querySelectorAll(".screen")];
const pushCard = document.getElementById("pushCard");
const themeToggle = document.getElementById("themeToggle");
const restartDemo = document.getElementById("restartDemo");
const confirmPaymentButton = document.getElementById("confirmPayment");
const showPushNowButton = document.getElementById("showPushNow");
const paymentOptions = [...document.querySelectorAll(".payment-option")];
const validitySelect = document.getElementById("validitySelect");
const plateInput = document.getElementById("plateInput");
const modelStatusCard = document.getElementById("modelStatusCard");
const modelProgressBar = document.getElementById("modelProgressBar");
const langCzButton = document.getElementById("langCz");
const langEnButton = document.getElementById("langEn");
const phoneScreen = document.querySelector(".phone-screen");

const translations = {
  cz: {
    brand_claim: "Digitální bankovnictví s kontextovou nabídkou",
    demo_kicker: "Clickable mobile demo",
    panel_title: "Nákup na edalnice.cz spouští push z Horizon AI bank s nabídkou pojištění vozidel.",
    panel_copy: "Demo simuluje externí nákup dálniční známky na webu `edalnice.cz`. Po úspěšné platbě banka rozpozná transakci, odešle push notifikaci a po kliknutí otevře klientovi personalizovanou nabídku v mobilní aplikaci.",
    theme_toggle: "Přepnout motiv",
    restart_demo: "Restart dema",
    note_trigger_label: "Trigger",
    note_trigger_title: "Dálniční známka",
    note_trigger_copy: "Platba na `edalnice.cz` vytvoří behaviorální signál pro vozidlo a aktivuje cross-sell logiku banky.",
    note_nba_label: "Next best action",
    note_nba_title: "Push od banky do appky",
    note_nba_copy: "Po úspěšné platbě přijde push od Horizon AI bank a proklik vede přímo do připravené nabídky.",
    welcome_text: "Ahoj, Kláro",
    home_eyebrow: "External purchase journey",
    home_title: "Klient jde na edalnice.cz koupit dálniční známku.",
    home_copy: "Demo začíná mimo bankovní prostředí. Banka vstupuje do scénáře až po zachycení karetní transakce a po vyhodnocení modelu.",
    home_button: "Pokračovat na edalnice.cz",
    merchant_eyebrow: "Externí web",
    merchant_title: "Elektronická dálniční známka",
    plate_label: "SPZ vozidla",
    validity_label: "Platnost",
    validity_annual: "Roční - 2 300 Kč",
    validity_monthly: "Měsíční - 430 Kč",
    validity_daily: "Desetidenní - 270 Kč",
    merchant_label: "Obchodník",
    amount_label: "Částka",
    merchant_button: "Pokračovat k platbě na edalnice.cz",
    payment_eyebrow: "Platba",
    payment_title: "Vyberte způsob úhrady",
    payment_fast: "Rychlá platba",
    payment_card_title: "Platební karta",
    payment_card_copy: "Zadání údajů o kartě",
    purchase_type_label: "Typ nákupu",
    purchase_type_value: "Dálniční známka",
    payment_label: "Platba",
    total_label: "Celkem",
    confirm_payment: "Zaplatit a potvrdit",
    success_eyebrow: "Platba na edalnice.cz",
    success_title: "Nákup dálniční známky byl úspěšně dokončen.",
    success_copy: "Banka rozpoznala karetní transakci jako vehicle signal. Teď může poslat push notifikaci s nabídkou pojištění vozidel.",
    merchant_enough_label: "Merchant",
    payment_method_label: "Platební metoda",
    model_eval_label: "Vyhodnocení modelu",
    model_eval_title: "Vyhodnocujeme vhodnost nabídky.",
    model_eval_copy: "Kontrolujeme transakční signál, mobilní aktivitu a pravděpodobnost zájmu o pojištění vozidla.",
    run_eval_button: "Spustit vyhodnocení a poslat push",
    offer_eyebrow: "Personalizovaná nabídka",
    offer_title: "Pojištění vozidel k vaší nové cestě.",
    offer_copy: "Připravili jsme pro vás předvyplněnou nabídku pojištění vozidla. Sjednání dokončíte online během několika minut.",
    offer_price_label: "Měsíčně od",
    offer_process_label: "Vyřízení",
    offer_process_value: "Online",
    offer_duration_label: "Odhad času",
    offer_duration_value: "2 min",
    benefit_1: "Asistence v ČR i zahraničí",
    benefit_2: "Rychlé hlášení škody v appce",
    benefit_3: "Sjednání do 2 minut s využitím stávajících dat klienta",
    offer_button: "Chci nabídku",
    back_home: "Zpět na přehled",
    push_now: "teď",
    push_title: "Máte v aplikaci novou nabídku pojištění k vozidlu.",
    push_copy: "Otevřete aplikaci a zobrazte si detail nabídky."
  },
  en: {
    brand_claim: "Digital banking with contextual offers",
    demo_kicker: "Clickable mobile demo",
    panel_title: "A purchase on edalnice.cz triggers a push from Horizon AI bank with a vehicle insurance offer.",
    panel_copy: "The demo simulates an external motorway vignette purchase on `edalnice.cz`. After a successful payment, the bank detects the transaction, sends a push notification, and after a tap opens a personalized offer in the mobile app.",
    theme_toggle: "Toggle theme",
    restart_demo: "Restart demo",
    note_trigger_label: "Trigger",
    note_trigger_title: "Motorway vignette",
    note_trigger_copy: "A payment on `edalnice.cz` creates a vehicle-related behavioral signal and activates the bank's cross-sell logic.",
    note_nba_label: "Next best action",
    note_nba_title: "Bank push into the app",
    note_nba_copy: "After a successful payment, the client receives a push from Horizon AI bank and the tap opens a prepared offer directly in the app.",
    welcome_text: "Hi, Klara",
    home_eyebrow: "External purchase journey",
    home_title: "The customer goes to edalnice.cz to buy a motorway vignette.",
    home_copy: "The demo starts outside the bank environment. The bank enters the journey only after the card transaction is detected and the model is evaluated.",
    home_button: "Continue to edalnice.cz",
    merchant_eyebrow: "External website",
    merchant_title: "Electronic motorway vignette",
    plate_label: "License plate",
    validity_label: "Validity",
    validity_annual: "Annual - CZK 2,300",
    validity_monthly: "Monthly - CZK 430",
    validity_daily: "10 days - CZK 270",
    merchant_label: "Merchant",
    amount_label: "Amount",
    merchant_button: "Continue to payment on edalnice.cz",
    payment_eyebrow: "Payment",
    payment_title: "Choose payment method",
    payment_fast: "Fast payment",
    payment_card_title: "Payment card",
    payment_card_copy: "Enter card details",
    purchase_type_label: "Purchase type",
    purchase_type_value: "Motorway vignette",
    payment_label: "Payment",
    total_label: "Total",
    confirm_payment: "Pay and confirm",
    success_eyebrow: "Payment on edalnice.cz",
    success_title: "The motorway vignette purchase was completed successfully.",
    success_copy: "The bank recognized the card transaction as a vehicle signal. It can now send a push notification with a vehicle insurance offer.",
    merchant_enough_label: "Merchant",
    payment_method_label: "Payment method",
    model_eval_label: "Model evaluation",
    model_eval_title: "Evaluating offer eligibility.",
    model_eval_copy: "We are checking the transaction signal, mobile activity, and the likelihood of interest in vehicle insurance.",
    run_eval_button: "Run evaluation and send push",
    offer_eyebrow: "Personalized offer",
    offer_title: "Vehicle insurance for your next trip.",
    offer_copy: "We have prepared a prefilled vehicle insurance offer for you. You can complete the application online in just a few minutes.",
    offer_price_label: "From per month",
    offer_process_label: "Processing",
    offer_process_value: "Online",
    offer_duration_label: "Estimated time",
    offer_duration_value: "2 min",
    benefit_1: "Roadside assistance in the Czech Republic and abroad",
    benefit_2: "Fast claim reporting in the app",
    benefit_3: "Complete in 2 minutes using existing customer data",
    offer_button: "I want the offer",
    back_home: "Back to overview",
    push_now: "now",
    push_title: "You have a new vehicle insurance offer in the app.",
    push_copy: "Open the app to view the offer details."
  }
};

const amountByValidity = {
  annual: "2 300 Kč",
  monthly: "430 Kč",
  daily: "270 Kč",
};

const state = {
  currentScreen: "home",
  selectedMethod: "Apple Pay",
  pushVisible: false,
  scoringInProgress: false,
  language: "cz",
};

function applyTranslations(language) {
  const dictionary = translations[language];
  document.documentElement.lang = language === "cz" ? "cs" : "en";
  document.title = language === "cz" ? "Horizon AI bank - Mobile Demo" : "Horizon AI bank - Mobile Demo";
  document.querySelectorAll("[data-i18n]").forEach((element) => {
    const key = element.dataset.i18n;
    if (dictionary[key]) {
      element.textContent = dictionary[key];
    }
  });
  langCzButton.classList.toggle("active", language === "cz");
  langEnButton.classList.toggle("active", language === "en");
}

function setLanguage(language) {
  state.language = language;
  applyTranslations(language);
}

function showScreen(screenName) {
  state.currentScreen = screenName;
  screens.forEach((screen) => {
    screen.classList.toggle("active", screen.dataset.screen === screenName);
  });
  phoneScreen.classList.toggle(
    "external-mode",
    ["merchant", "payment", "success"].includes(screenName)
  );
}

function updateAmounts() {
  const amount = amountByValidity[validitySelect.value];
  document.getElementById("vignetteAmount").textContent = amount;
  document.getElementById("paymentAmount").textContent = amount;
  document.getElementById("receiptAmount").textContent = amount;
}

function updatePlate() {
  document.getElementById("platePreview").textContent = plateInput.value || "2AB 4587";
}

function updateMethod(method) {
  state.selectedMethod = method;
  paymentOptions.forEach((option) => {
    option.classList.toggle("selected", option.dataset.method === method);
  });
  document.getElementById("paymentMethodLabel").textContent = method;
  document.getElementById("receiptMethod").textContent = method;
}

function showPush() {
  state.pushVisible = true;
  pushCard.classList.remove("hidden");
}

function hidePush() {
  state.pushVisible = false;
  pushCard.classList.add("hidden");
}

function resetDemo() {
  hidePush();
  state.scoringInProgress = false;
  modelStatusCard.classList.add("hidden");
  modelProgressBar.style.width = "0%";
  updateMethod("Apple Pay");
  validitySelect.value = "annual";
  plateInput.value = "2AB 4587";
  updateAmounts();
  updatePlate();
  showScreen("home");
}

function runModelAndSendPush() {
  if (state.scoringInProgress) {
    return;
  }
  state.scoringInProgress = true;
  hidePush();
  modelStatusCard.classList.remove("hidden");
  modelProgressBar.style.width = "0%";

  window.setTimeout(() => {
    modelProgressBar.style.width = "78%";
  }, 80);

  window.setTimeout(() => {
    modelProgressBar.style.width = "100%";
  }, 1200);

  window.setTimeout(() => {
    state.scoringInProgress = false;
    showPush();
  }, 1850);
}

document.querySelectorAll("[data-next]").forEach((button) => {
  button.addEventListener("click", () => {
    const next = button.dataset.next;
    updateAmounts();
    updatePlate();
    showScreen(next);
  });
});

document.querySelectorAll("[data-back]").forEach((button) => {
  button.addEventListener("click", () => {
    const back = button.dataset.back;
    if (back !== "offer") {
      hidePush();
    }
    showScreen(back);
  });
});

paymentOptions.forEach((option) => {
  option.addEventListener("click", () => updateMethod(option.dataset.method));
});

confirmPaymentButton.addEventListener("click", () => {
  updateAmounts();
  updatePlate();
  showScreen("success");
  window.setTimeout(runModelAndSendPush, 650);
});

showPushNowButton.addEventListener("click", runModelAndSendPush);

pushCard.addEventListener("click", () => {
  hidePush();
  showScreen("offer");
});

pushCard.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    hidePush();
    showScreen("offer");
  }
});

themeToggle.addEventListener("click", () => {
  const nextTheme = body.dataset.theme === "light" ? "dark" : "light";
  body.dataset.theme = nextTheme;
});

restartDemo.addEventListener("click", resetDemo);
validitySelect.addEventListener("change", updateAmounts);
plateInput.addEventListener("input", updatePlate);
langCzButton.addEventListener("click", () => setLanguage("cz"));
langEnButton.addEventListener("click", () => setLanguage("en"));

updateAmounts();
updatePlate();
updateMethod("Apple Pay");
applyTranslations(state.language);
showScreen("home");
