import { EditorView } from '@codemirror/view';

// Dark theme with rim highlights for CodeMirror
const darkBackground = '#12161D';
const darkGray = '#12161D';
const mediumGray = '#1F242E';
const accentPink = '#ff5fa8';
const textPrimary = '#E6EDF3';
const textSecondary = '#A7B0C0';

export const colabTheme = EditorView.theme({
  '&': {
    backgroundColor: `${darkBackground} !important`,
    color: textPrimary,
    border: `1px solid ${mediumGray} !important`,
  },
  '.cm-editor': {
    backgroundColor: `${darkBackground} !important`,
    border: `1px solid ${mediumGray} !important`,
  },
  '.cm-editor.cm-focused': {
    outline: `2px solid ${accentPink} !important`,
    outlineOffset: '-1px',
  },
  '.cm-scroller': {
    fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, 'Courier New', monospace",
  },
  '.cm-content': {
    padding: '8px 0',
    fontSize: '0.875rem',
    fontFamily: "'JetBrains Mono', 'Fira Code', Menlo, Monaco, Consolas, 'Courier New', monospace",
    color: `${textPrimary} !important`,
  },
  '.cm-focused': {
    outline: 'none',
  },
  '.cm-gutters': {
    backgroundColor: `${darkGray} !important`,
    border: 'none',
    borderRight: `1px solid ${mediumGray} !important`,
    color: `${textSecondary} !important`,
  },
  '.cm-lineNumbers .cm-gutterElement': {
    padding: '0 8px',
    fontSize: '0.8125rem',
    color: `${textSecondary} !important`,
  },
  '.cm-activeLineGutter': {
    backgroundColor: `${darkGray} !important`,
    color: `${textSecondary} !important`,
  },
  '.cm-activeLine': {
    backgroundColor: `${darkGray} !important`,
  },
  '.cm-selectionBackground': {
    backgroundColor: `${mediumGray} !important`,
  },
  '.cm-keyword': { color: `${accentPink} !important`, fontWeight: 'bold' },
  '.cm-string': { color: '#C4B5FD !important' },
  '.cm-number': { color: '#93C5FD !important' },
  '.cm-operator': { color: `${textPrimary} !important` },
  '.cm-variable': { color: '#E6EDF3 !important' },
  '.cm-builtin': { color: '#8BD5CA !important' },
  '.cm-comment': { color: `${textSecondary} !important`, fontStyle: 'italic' },
});
