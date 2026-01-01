// خدمة للتعامل مع الذاكرة المحلية
const storageService = {
    getTerms: function() {
        return new Promise((resolve) => {
            chrome.storage.local.get(['medicalTerms'], (result) => {
                const terms = result.medicalTerms || [];
                console.log('Loaded terms:', terms);
                resolve(terms);
            });
        });
    },

    saveTerms: function(terms) {
        return new Promise((resolve) => {
            if (!Array.isArray(terms)) {
                console.error('Invalid terms format:', terms);
                resolve(false);
                return;
            }

            // Remove duplicates and sort
            const uniqueTerms = [...new Set(terms)].sort();

            chrome.storage.local.set({ medicalTerms: uniqueTerms }, () => {
                console.log('Saved terms:', uniqueTerms);
                resolve(true);
            });
        });
    }
};

// فتح نافذة جديدة عند النقر على أيقونة الإضافة
chrome.action.onClicked.addListener(() => {
    chrome.tabs.create({
        url: chrome.runtime.getURL("popup.html")
    });
});

// إضافة خدمة النقل
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === 'getTerms') {
        storageService.getTerms().then(terms => {
            sendResponse({ terms: terms });
        });
        return true; // نحتاج إلى الاحتفاظ بالاتصال حتى يتم الرد
    } else if (request.action === 'saveTerms') {
        if (request.terms) {
            storageService.saveTerms(request.terms).then(success => {
                sendResponse({ success: success });
            });
            return true;
        }
    }
    return false;
});