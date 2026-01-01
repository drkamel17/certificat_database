// Script pour gérer la taille de police à l'impression
function setupFontSizeControl(fontSizeId) {
    // Vérifier si DOMContentLoaded a déjà eu lieu
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            initFontSizeControl(fontSizeId);
        });
    } else {
        // DOMContentLoaded a déjà eu lieu, initialiser directement
        initFontSizeControl(fontSizeId);
    }
}

function initFontSizeControl(fontSizeId) {
    const fontSizeInput = document.getElementById(fontSizeId);
    if (fontSizeInput) {
        // Appliquer la taille de police initiale
        updateFontSize(fontSizeInput.value);

        // Ajouter l'écouteur pour les changements
        fontSizeInput.addEventListener('input', function() {
            updateFontSize(this.value);
        });
    }
}

function updateFontSize(fontSize) {
    // Appliquer la taille de police au contenu du certificat
    const styleElement = document.createElement('style');
    styleElement.textContent = '@media print {' +
        '.certificat {' +
        'font-size: ' + fontSize + 'pt !important;' +
        '}' +
        '.certificat p {' +
        'font-size: ' + fontSize + 'pt !important;' +
        '}' +
        '.certificat input[type="text"], ' +
        '.certificat input[type="date"], ' +
        '.certificat textarea {' +
        'font-size: ' + fontSize + 'pt !important;' +
        '}' +
        '}';

    // Supprimer l'ancien style s'il existe
    const oldStyle = document.getElementById('fontSizeStyle');
    if (oldStyle) {
        oldStyle.remove();
    }

    // Ajouter le nouveau style
    styleElement.id = 'fontSizeStyle';
    document.head.appendChild(styleElement);
}