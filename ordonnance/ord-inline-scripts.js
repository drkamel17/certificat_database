// Fonction pour capitaliser automatiquement les noms et prénoms
function capitalizeNames(text) {
    if (!text) return text;
    return text.toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());
}

// Ajouter des écouteurs pour capitaliser et sauvegarder automatiquement les noms
document.addEventListener('DOMContentLoaded', function() {
    const nomField = document.getElementById('nom');
    const prenomField = document.getElementById('prenom');
    const poidsField = document.getElementById('poids');

    // Capitalisation et sauvegarde automatique pour le champ nom
    if (nomField) {
        nomField.addEventListener('input', function() {
            const capitalizedName = capitalizeNames(this.value);
            localStorage.setItem('nom', capitalizedName);
            // Mettre à jour le champ sans déplacer le curseur
            if (this.value !== capitalizedName) {
                const cursorPos = this.selectionStart;
                this.value = capitalizedName;
                this.setSelectionRange(cursorPos, cursorPos);
            }
        });
    }

    // Capitalisation et sauvegarde automatique pour le champ prénom
    if (prenomField) {
        prenomField.addEventListener('input', function() {
            const capitalizedName = capitalizeNames(this.value);
            localStorage.setItem('prenom', capitalizedName);
            // Mettre à jour le champ sans déplacer le curseur
            if (this.value !== capitalizedName) {
                const cursorPos = this.selectionStart;
                this.value = capitalizedName;
                this.setSelectionRange(cursorPos, cursorPos);
            }
        });
    }

    // Sauvegarde automatique pour le champ poids
    if (poidsField) {
        poidsField.addEventListener('input', function() {
            localStorage.setItem('poids', this.value);
        });
    }

    // Charger la valeur du poids depuis localStorage
    const savedPoids = localStorage.getItem('poids');
    if (savedPoids && poidsField) {
        poidsField.value = savedPoids;
    }
});