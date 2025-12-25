// Ce script ajoute le CSS pour masquer les boutons lors de l'impression
(function() {
    // Ajouter le CSS global à toutes les pages
    const style = document.createElement('style');
    style.textContent = `
        @media print {
            /* Masquer les contrôles d'impression et de sauvegarde */
            .print-button div[style*="align-items: center"],
            .print-button button {
                display: none !important;
            }
        }
    `;
    
    // Ajouter au document actuel
    if (document.head) {
        document.head.appendChild(style);
    }
    
    // Ajouter à toutes les nouvelles fenêtres qui s'ouvrent
    const originalOpen = window.open;
    window.open = function() {
        const win = originalOpen.apply(this, arguments);
        if (win && win.document) {
            setTimeout(() => {
                if (win.document.head) {
                    const newStyle = document.createElement('style');
                    newStyle.textContent = style.textContent;
                    win.document.head.appendChild(newStyle);
                }
            }, 100);
        }
        return win;
    };
})();