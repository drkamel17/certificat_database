// JsBarcode configuration
window.JsBarcode = function(element, value, options) {
    if (!element) {
        throw new Error("Element not found");
    }

    this.element = element;
    this.value = value || "";
    this.options = options || {};

    return this;
};

JsBarcode.prototype.code128 = function(value) {
    this.value = value;
    return this;
};

JsBarcode.prototype.render = function() {
    // Simple mock implementation for testing
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("version", "1.1");
    svg.setAttribute("viewBox", "0 0 300 100");
    // Fixed the template literal by ensuring proper formatting
    svg.innerHTML = `
        <rect width="300" height="100" fill="#FFFFFF"/>
        <text x="50%" y="50%" text-anchor="middle" fill="#000000">${this.value}</text>
    `;
    this.element.appendChild(svg);
};