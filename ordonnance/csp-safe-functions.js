// Fonctions sécurisées pour remplacer document.write et éviter les violations CSP

// Fonction pour nettoyer le contenu HTML des scripts inline
function cleanInlineScripts(htmlContent) {
    if (!htmlContent) return htmlContent;
    
    // Supprimer tous les scripts inline
    let cleanContent = htmlContent.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '');
    
    // Supprimer les attributs d'événements inline (onclick, onload, etc.)
    cleanContent = cleanContent.replace(/\s+on\w+\s*=\s*["'][^"']*["']/gi, '');
    
    return cleanContent;
}

// Fonction de remplacement sécurisée pour document.write
function safeDocumentWrite(windowObj, content) {
    if (!windowObj || !windowObj.document) {
        console.warn('Fenêtre invalide pour safeDocumentWrite');
        return;
    }
    
    try {
        // Nettoyer le contenu des scripts inline
        const cleanContent = cleanInlineScripts(content);
        
        // Utiliser document.write avec le contenu nettoyé
        windowObj.document.open();
        windowObj.document.write(cleanContent);
        windowObj.document.close();
        
        // Ajouter les scripts nécessaires de manière sécurisée
        setTimeout(() => addSafeScripts(windowObj.document), 100);
        
    } catch (error) {
        console.warn('Erreur lors de l\'écriture sécurisée du document:', error);
        // Fallback: utiliser une URL blob
        try {
            const blob = new Blob([cleanInlineScripts(content)], { type: 'text/html' });
            const url = URL.createObjectURL(blob);
            windowObj.location.href = url;
            setTimeout(() => URL.revokeObjectURL(url), 1000);
        } catch (blobError) {
            console.error('Erreur fallback blob:', blobError);
        }
    }
}

// Fonction pour ajouter les scripts nécessaires de manière sécurisée
function addSafeScripts(doc) {
    if (!doc || !doc.head) return;
    
    // Ajouter les scripts externes nécessaires
    const scripts = [
        'browser-polyfill.js',
        'JsBarcode.all.min.js',
        'barcode.js',
        'print.js'
    ];
    
    scripts.forEach(scriptSrc => {
        try {
            const script = doc.createElement('script');
            script.src = chrome.runtime.getURL(scriptSrc);
            script.onerror = () => console.warn(`Impossible de charger ${scriptSrc}`);
            doc.head.appendChild(script);
        } catch (error) {
            console.warn(`Erreur lors de l'ajout du script ${scriptSrc}:`, error);
        }
    });
}

// Fonction pour ouvrir une fenêtre avec du contenu sécurisé
function openSafeWindow(htmlContent, windowName = '_blank') {
    const newWindow = window.open('', windowName);
    if (newWindow) {
        safeDocumentWrite(newWindow, htmlContent);
    }
    return newWindow;
}

// Remplacer globalement les fonctions problématiques
window.safeDocumentWrite = safeDocumentWrite;
window.openSafeWindow = openSafeWindow;
window.cleanInlineScripts = cleanInlineScripts;

// Intercepter et remplacer document.write globalement
(function() {
    const originalWrite = Document.prototype.write;
    const originalWriteln = Document.prototype.writeln;
    
    Document.prototype.write = function(content) {
        const cleanContent = cleanInlineScripts(content);
        return originalWrite.call(this, cleanContent);
    };
    
    Document.prototype.writeln = function(content) {
        const cleanContent = cleanInlineScripts(content);
        return originalWriteln.call(this, cleanContent);
    };
    
    // Intercepter window.open pour utiliser la version sécurisée
    const originalOpen = window.open;
    window.open = function(url, name, features) {
        if (url === '' || !url) {
            // Si c'est une fenêtre vide, retourner la fenêtre normale
            // Le contenu sera nettoyé par document.write intercepté
            return originalOpen.call(this, url, name, features);
        }
        return originalOpen.call(this, url, name, features);
    };
})();

console.log('CSP-safe functions loaded and document.write intercepted');