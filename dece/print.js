// print.js
document.addEventListener('DOMContentLoaded', function() {
    document.querySelector('.print-button button').addEventListener('click', function () {
        alert('Veuillez vérifier que le format est A4 et les marges sont "aucune".\n\nAjustez l\'échelle  pour imprimer sur une seule page.');
        window.print();
    });
});


