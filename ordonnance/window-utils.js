// Utilitaire pour ouvrir des fenêtres avec du contenu HTML sans violer le CSP
function openWindowWithContent(htmlContent, windowName = '_blank') {
    // Créer un blob avec le contenu HTML
    const blob = new Blob([htmlContent], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    
    // Ouvrir la fenêtre avec l'URL du blob
    const newWindow = window.open(url, windowName);
    
    // Nettoyer l'URL du blob après un délai
    setTimeout(() => {
        URL.revokeObjectURL(url);
    }, 1000);
    
    return newWindow;
}

// Fonction de remplacement pour document.write qui respecte le CSP
function safeDocumentWrite(windowObj, content) {
    if (windowObj && windowObj.document) {
        try {
            // Utiliser innerHTML au lieu de document.write
            windowObj.document.documentElement.innerHTML = content;
        } catch (error) {
            console.warn('Erreur lors de l\'écriture du document:', error);
            // Fallback: utiliser la méthode blob
            windowObj.location.href = URL.createObjectURL(new Blob([content], { type: 'text/html' }));
        }
    }
}

// Exporter les fonctions pour utilisation globale
window.openWindowWithContent = openWindowWithContent;
window.safeDocumentWrite = safeDocumentWrite;