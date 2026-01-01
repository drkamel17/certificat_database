// --- Service pour gérer la mémoire locale ---
const storageService = {
    getTerms: function () {
        return new Promise((resolve) => {
            chrome.storage.local.get(['medicalTerms'], (result) => {
                const terms = result.medicalTerms || [];
                console.log('Loaded terms:', terms);
                resolve(terms);
            });
        });
    },

    saveTerms: function (terms) {
        return new Promise((resolve) => {
            if (!Array.isArray(terms)) {
                console.error('Invalid terms format:', terms);
                resolve(false);
                return;
            }

            const uniqueTerms = [...new Set(terms)].sort();

            chrome.storage.local.set({ medicalTerms: uniqueTerms }, () => {
                console.log('Saved terms:', uniqueTerms);
                resolve(true);
            });
        });
    }
};

// --- Quand on clique sur l’icône ---
chrome.action.onClicked.addListener(() => {
    chrome.tabs.create({
        url: chrome.runtime.getURL("popup.html")
    });
});

// --- Listener messages ---
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === 'getTerms') {
        storageService.getTerms().then(terms => {
            sendResponse({ terms: terms });
        });
        return true;

    } else if (request.action === 'saveTerms') {
        if (request.terms) {
            storageService.saveTerms(request.terms).then(success => {
                sendResponse({ success: success });
            });
            return true;
        }

    } else if (request.action === 'extractInfo' && request.tabId) {
        chrome.scripting.executeScript(
            {
                target: { tabId: request.tabId },
                func: extractAndStoreInfo
            },
            (results) => {
                if (chrome.runtime.lastError) {
                    console.error("Erreur injection:", chrome.runtime.lastError.message);
                    sendResponse({ data: null });
                    return;
                }
                if (results && results[0] && results[0].result) {
                    sendResponse({ data: results[0].result });
                } else {
                    sendResponse({ data: null });
                }
            }
        );
        return true; // async
    }

    return false;
});

// --- Fonction injectée dans la page active ---
function extractAndStoreInfo() {
    const nom = document.querySelector("c[type='nom']")?.innerText || "";
    const prenom = document.querySelector("c[type='prenom']")?.innerText || "";
    const dobElement = document.querySelector(".widget-user-desc");
    const dob = dobElement ? dobElement.innerText.replace("Date de naissance : ", "").trim() : "";

    let numero = "";
    document.querySelectorAll("td").forEach(td => {
        if (td.textContent.includes("Numero :")) {
            numero = td.nextElementSibling ? td.nextElementSibling.innerText : "";
        }
    });

    let etablissementFr = 'Nom français non trouvé';
    let etablissementAr = 'Nom arabe non trouvé';

    const scriptTags = document.getElementsByTagName('script');
    for (let i = 0; i < scriptTags.length; i++) {
        const script = scriptTags[i];
        if (script.textContent) {
            const regex = /<tr>.*?<td[^>]*colspan="2"[^>]*>(.*?)<\/td>.*?<td[^>]*colspan="2"[^>]*rowspan="3"[^>]*>(.*?)<\/td>.*?<\/tr>/is;
            const match = script.textContent.match(regex);
            if (match) {
                etablissementFr = match[1].trim();
                etablissementAr = match[2].trim();
                break;
            }
        }
    }

    return { nom, prenom, dob, numero, etablissementFr, etablissementAr };
}
