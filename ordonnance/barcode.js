// Fonction pour générer le code-barres
function genererCodeBarres() {
    const numero = document.querySelector('.info.numero');
    if (numero) {
        const numeroText = numero.textContent.split(':')[1]?.trim() || '';
        const barcodeDiv = document.querySelector('.info.barcode');
        if (barcodeDiv && numeroText && numeroText.length > 0) {
            // Créer un élément SVG pour le code-barres
            const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            svg.setAttribute('id', 'barcode');
            barcodeDiv.appendChild(svg);

            try {
                // Générer le code-barres avec JsBarcode seulement si le numéro est valide
                JsBarcode(svg, numeroText, {
                    format: "CODE128",
                    lineColor: "#000",
                    width: 2,
                    height: 50,
                    displayValue: false
                });
            } catch (error) {
                console.warn('Erreur lors de la génération du code-barres:', error);
                // Supprimer le SVG en cas d'erreur
                svg.remove();
            }
        }
    }
}

// Appeler la fonction une fois le DOM chargé
document.addEventListener('DOMContentLoaded', genererCodeBarres);
