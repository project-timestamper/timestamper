// ==UserScript==
// @name         USPTO
// @namespace    http://tampermonkey.net/
// @version      2025-12-28
// @description  try to take over the world!
// @author       You
// @match        https://data.uspto.gov/bulkdata/datasets*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=uspto.gov
// @grant        none
// ==/UserScript==

const originalFillText = Object.getOwnPropertyDescriptor(CanvasRenderingContext2D.prototype, "fillText").value;

(function() {
  'use strict';
  Object.defineProperty(CanvasRenderingContext2D.prototype, "fillText", {
    value: function (...args) {
        console.log("fillText", args);
        originalFillText.call(this, ...args);
        const [a, _1, b, _2] = args[0].split(/\s+/);
        const answer = parseInt(a) + parseInt(b);
        const input = document.querySelector('input#jCaptcha')
        input.value = String(answer)
        const continueButton = document.querySelector('app-captcha-dialog button.btn-primary')
        setTimeout(() => continueButton.click(), 100);
    }
  });
  console.log("hi");
  // Your code here...
})();
