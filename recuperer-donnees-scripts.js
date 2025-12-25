// Scripts supplémentaires pour la page de récupération de données
document.addEventListener('DOMContentLoaded', function() {
    
    // Ajouter des styles CSS pour l'impression
    const printStyles = document.createElement('style');
    printStyles.textContent = `
        @media print {
            body * {
                visibility: hidden;
            }
            #results, #results * {
                visibility: visible;
            }
            #results {
                position: absolute;
                left: 0;
                top: 0;
                width: 100%;
            }
            .container {
                box-shadow: none;
                padding: 0;
            }
            h1, h2 {
                page-break-after: avoid;
            }
            table {
                page-break-inside: auto;
            }
            tr {
                page-break-inside: avoid;
                page-break-after: auto;
            }
            td, th {
                page-break-inside: avoid;
                page-break-after: auto;
            }
            #exportExcel, #printTable, .form-group, #successMessage, #errorMessage, #loading {
                display: none !important;
            }
        }
    `;
    document.head.appendChild(printStyles);
    
    // Améliorer l'expérience utilisateur avec des raccourcis clavier
    document.addEventListener('keydown', function(e) {
        // Ctrl+Enter pour soumettre le formulaire
        if (e.ctrlKey && e.key === 'Enter') {
            const form = document.getElementById('dateForm');
            if (form) {
                form.dispatchEvent(new Event('submit'));
            }
        }
        
        // Ctrl+E pour exporter vers Excel
        if (e.ctrlKey && e.key === 'e') {
            e.preventDefault();
            const exportBtn = document.getElementById('exportExcel');
            if (exportBtn && exportBtn.style.display !== 'none') {
                exportBtn.click();
            }
        }
        
        // Ctrl+P pour imprimer
        if (e.ctrlKey && e.key === 'p') {
            e.preventDefault();
            const printBtn = document.getElementById('printTable');
            if (printBtn && printBtn.style.display !== 'none') {
                printBtn.click();
            }
        }
        
        // Échap pour réinitialiser le formulaire
        if (e.key === 'Escape') {
            const form = document.getElementById('dateForm');
            if (form) {
                form.reset();
                document.getElementById('results').style.display = 'none';
                document.getElementById('successMessage').style.display = 'none';
                document.getElementById('errorMessage').style.display = 'none';
            }
        }
    });
    
    // Ajouter une fonction pour préremplir les dates avec le mois courant
    function setDatesToCurrentMonth() {
        const now = new Date();
        const firstDay = new Date(now.getFullYear(), now.getMonth(), 1);
        const lastDay = new Date(now.getFullYear(), now.getMonth() + 1, 0);
        
        document.getElementById('dateDebut').value = firstDay.toISOString().split('T')[0];
        document.getElementById('dateFin').value = lastDay.toISOString().split('T')[0];
    }
    
    // Ajouter un bouton pour préremplir avec le mois courant
    const dateForm = document.getElementById('dateForm');
    if (dateForm) {
        const monthButton = document.createElement('button');
        monthButton.type = 'button';
        monthButton.textContent = 'Mois courant';
        monthButton.style.cssText = 'background-color: #6c757d; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; margin-top: 10px; width: auto;';
        monthButton.addEventListener('click', setDatesToCurrentMonth);
        
        // Insérer le bouton après le bouton de soumission
        const submitButton = dateForm.querySelector('button[type="submit"]');
        if (submitButton) {
            submitButton.parentNode.insertBefore(monthButton, submitButton.nextSibling);
        }
    }
    
    // Ajouter des statistiques sur les résultats
    function addStatistics(data, table) {
        if (!data || data.length === 0) return;
        
        const statsDiv = document.createElement('div');
        statsDiv.style.cssText = 'margin-top: 20px; padding: 15px; background-color: #e9ecef; border-radius: 4px;';
        
        let statsHtml = '<h3>Statistiques</h3>';
        statsHtml += `<p>Total des enregistrements: ${data.length}</p>`;
        
        // Statistiques spécifiques selon la table
        if (table === 'arrets_travail' || table === 'prolongation') {
            const totalJours = data.reduce((sum, row) => sum + (parseInt(row.nombre_jours) || 0), 0);
            statsHtml += `<p>Total des jours d'arrêt: ${totalJours}</p>`;
            statsHtml += `<p>Moyenne de jours par arrêt: ${(totalJours / data.length).toFixed(1)}</p>`;
        } else if (table === 'cbv') {
            const uniqueDoctors = [...new Set(data.map(row => row.medecin).filter(Boolean))];
            statsHtml += `<p>Nombre de médecins: ${uniqueDoctors.length}</p>`;
            statsHtml += `<p>Médecins: ${uniqueDoctors.join(', ')}</p>`;
        } else if (table === 'antirabique') {
            const vaccineTypes = [...new Set(data.map(row => row.type_de_vaccin).filter(Boolean))];
            statsHtml += `<p>Types de vaccins: ${vaccineTypes.join(', ')}</p>`;
            const animals = [...new Set(data.map(row => row.animal).filter(Boolean))];
            statsHtml += `<p>Animaux: ${animals.join(', ')}</p>`;
        }
        
        statsDiv.innerHTML = statsHtml;
        
        // Ajouter les statistiques après le tableau
        const resultsContent = document.getElementById('resultsContent');
        if (resultsContent) {
            resultsContent.appendChild(statsDiv);
        }
    }
    
    // Surcharge de la fonction afficherResultats pour inclure les statistiques
    const originalAfficherResultats = window.afficherResultats;
    if (originalAfficherResultats) {
        window.afficherResultats = function(data, table) {
            originalAfficherResultats(data, table);
            addStatistics(data, table);
        };
    }
    
    // Ajouter une fonction de recherche dans les résultats
    function addSearchFunctionality() {
        const resultsDiv = document.getElementById('results');
        if (!resultsDiv) return;
        
        // Vérifier si la recherche existe déjà
        let searchDiv = document.getElementById('searchDiv');
        let searchInput = document.getElementById('searchInput');
        
        if (!searchDiv) {
            // Créer la div de recherche si elle n'existe pas
            searchDiv = document.createElement('div');
            searchDiv.id = 'searchDiv';
            searchDiv.style.cssText = 'margin-bottom: 15px;';
            searchDiv.innerHTML = `
                <input type="text" id="searchInput" placeholder="Rechercher dans les résultats..." 
                       style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box;">
            `;
            
            // Insérer avant le contenu des résultats
            const resultsContent = document.getElementById('resultsContent');
            if (resultsContent) {
                resultsDiv.insertBefore(searchDiv, resultsContent);
            }
        }
        
        // Ajouter ou réinitialiser la fonctionnalité de recherche
        searchInput = document.getElementById('searchInput');
        if (searchInput) {
            // Supprimer l'ancien écouteur d'événement s'il existe
            const newSearchInput = searchInput.cloneNode(true);
            searchInput.parentNode.replaceChild(newSearchInput, searchInput);
            
            // Ajouter le nouvel écouteur d'événement
            newSearchInput.addEventListener('input', function() {
                const searchTerm = this.value.toLowerCase();
                const resultsContent = document.getElementById('resultsContent');
                const table = resultsContent.querySelector('table');
                if (!table) return;
                
                const rows = table.querySelectorAll('tbody tr');
                rows.forEach(row => {
                    const text = row.textContent.toLowerCase();
                    // Ne pas cacher les lignes qui correspondent à la recherche
                    row.style.display = text.includes(searchTerm) ? '' : 'none';
                });
                
                // Si la recherche est vide, s'assurer que toutes les lignes sont visibles
                if (searchTerm === '') {
                    rows.forEach(row => {
                        row.style.display = '';
                    });
                }
            });
        }
    }
    
    // Fonction pour nettoyer la recherche
    function clearSearch() {
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.value = '';
        }
    }
    
    // Rendre la fonction accessible globalement
    window.clearSearch = clearSearch;
    
    // Initialiser la recherche lorsque les résultats sont affichés
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.type === 'attributes' && mutation.attributeName === 'style') {
                const results = document.getElementById('results');
                const searchDiv = document.getElementById('searchDiv');
                
                if (results && results.style.display === 'block') {
                    addSearchFunctionality();
                    // Afficher la recherche
                    if (searchDiv) {
                        searchDiv.style.display = 'block';
                    }
                } else {
                    // Masquer la recherche quand les résultats sont masqués
                    if (searchDiv) {
                        searchDiv.style.display = 'none';
                    }
                }
            }
        });
    });
    
    const resultsDiv = document.getElementById('results');
    if (resultsDiv) {
        observer.observe(resultsDiv, { attributes: true });
    }
    
    console.log('Scripts supplémentaires de récupération de données chargés');
});