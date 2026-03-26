import { useEffect } from 'react';
import legacyConsoleHtml from '../../index.html?raw';
import { initializeOperatorConsole } from '../../assets/operator-console.js';

const consoleMarkup = (() => {
  const bodyMatch = legacyConsoleHtml.match(/<body[^>]*>([\s\S]*)<\/body>/i);
  const bodyHtml = bodyMatch ? bodyMatch[1] : legacyConsoleHtml;
  return bodyHtml
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .trim();
})();

export default function App() {
  useEffect(() => {
    initializeOperatorConsole();
  }, []);

  return <div className="react-shell-root" dangerouslySetInnerHTML={{ __html: consoleMarkup }} />;
}
