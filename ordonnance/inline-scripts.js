// Scripts qui étaient précédemment inline

// Initialisation des écouteurs d'événements pour les boutons
document.addEventListener('DOMContentLoaded', function() {
    // Gestion des boutons de format
    const formatAvecEntete = document.getElementById('formatAvecEntete');
    const formatSansEntete = document.getElementById('formatSansEntete');
    
    if (formatAvecEntete) {
        formatAvecEntete.addEventListener('click', function() {
            localStorage.setItem('certificatFormat', 'avecEntete');
            updateFormatUI('avecEntete');
        });
    }
    
    if (formatSansEntete) {
        formatSansEntete.addEventListener('click', function() {
            localStorage.setItem('certificatFormat', 'sansEntete');
            updateFormatUI('sansEntete');
        });
    }
    
    // Restaurer le format sauvegardé
    const savedFormat = localStorage.getItem('certificatFormat') || 'avecEntete';
    updateFormatUI(savedFormat);
});

// Mise à jour de l'interface utilisateur en fonction du format sélectionné
function updateFormatUI(format) {
    const formatAvecEntete = document.getElementById('formatAvecEntete');
    const formatSansEntete = document.getElementById('formatSansEntete');
    
    if (format === 'avecEntete') {
        if (formatAvecEntete) formatAvecEntete.classList.add('active');
        if (formatSansEntete) formatSansEntete.classList.remove('active');
    } else {
        if (formatAvecEntete) formatAvecEntete.classList.remove('active');
        if (formatSansEntete) formatSansEntete.classList.add('active');
    }
}

// Sauvegarde des informations de la polyclinique et du docteur
document.addEventListener('DOMContentLoaded', function() {
    const saveButton = document.getElementById('SavePolycliniqueDocteur');
    if (saveButton) {
        saveButton.addEventListener('click', function() {
            const polyclinique = document.getElementById('polyclinique').value;
            const polycliniqueAr = document.getElementById('polyclinique-ar').value;
            const docteur = document.getElementById('docteur').value;
            
            localStorage.setItem('polyclinique', polyclinique);
            localStorage.setItem('polyclinique-ar', polycliniqueAr);
            localStorage.setItem('docteur', docteur);
            
            alert('Informations enregistrées avec succès !');
        });
    }
    
    // Restaurer les valeurs sauvegardées
    document.getElementById('polyclinique').value = localStorage.getItem('polyclinique') || '';
    document.getElementById('polyclinique-ar').value = localStorage.getItem('polyclinique-ar') || '';
    document.getElementById('docteur').value = localStorage.getItem('docteur') || '';
});
