// Fonction pour mettre à jour la taille de police et imprimer
function updateAndPrint() {
    // Récupérer la taille de police sélectionnée ou entrée
    var policeInput = document.getElementById('numero-police-impression');
    var taillePolice = policeInput ? policeInput.value : '12';
    
    // Valider que la taille est un nombre entre 8 et 24
    var tailleNumerique = parseInt(taillePolice);
    if (isNaN(tailleNumerique) || tailleNumerique < 8 || tailleNumerique > 24) {
        tailleNumerique = 12; // Valeur par défaut si invalide
        policeInput.value = tailleNumerique;
    }
    
    // Mettre à jour la variable CSS root
    document.documentElement.style.setProperty('--med-font-size', tailleNumerique + 'px');
    
    // Sauvegarder dans localStorage
    localStorage.setItem('taillePolice', tailleNumerique);
    
    // Imprimer après avoir mis à jour la taille de police
    window.print();
}

// Mettre à jour la taille de police en temps réel
document.addEventListener('DOMContentLoaded', function() {
    var policeInput = document.getElementById('numero-police-impression');
    if (policeInput) {
        // Charger la taille de police sauvegardée
        var tailleSauvegardee = localStorage.getItem('taillePolice');
        if (tailleSauvegardee) {
            policeInput.value = tailleSauvegardee;
        }
        
        // Écouter les changements (pour le select et l'input direct)
        policeInput.addEventListener('change', function() {
            var taillePolice = this.value || '12';
            var tailleNumerique = parseInt(taillePolice);
            
            // Valider que la taille est un nombre entre 8 et 24
            if (isNaN(tailleNumerique) || tailleNumerique < 8 || tailleNumerique > 24) {
                tailleNumerique = 12; // Valeur par défaut si invalide
                this.value = tailleNumerique;
            }
            
            document.documentElement.style.setProperty('--med-font-size', tailleNumerique + 'px');
            localStorage.setItem('taillePolice', tailleNumerique);
        });
        
        // Initialiser la taille de police
        var taillePolice = policeInput.value || '12';
        var tailleNumerique = parseInt(taillePolice);
        document.documentElement.style.setProperty('--med-font-size', tailleNumerique + 'px');
    }
});
