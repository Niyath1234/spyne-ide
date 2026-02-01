import { EditorView } from '@codemirror/view';

// Colab Dark Theme for CodeMirror
export const colabTheme = EditorView.theme({
  '&': {
    backgroundColor: '#1f1f1f !important',
    color: '#e8eaed',
  },
  '.cm-editor': {
    backgroundColor: '#1f1f1f !important',
  },
  '.cm-scroller': {
    fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
  },
  '.cm-content': {
    padding: '8px 0',
    fontSize: '0.875rem',
    fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
    color: '#e8eaed !important',
  },
  '.cm-focused': {
    outline: 'none',
  },
  '.cm-gutters': {
    backgroundColor: '#1f1f1f !important',
    border: 'none',
    color: '#7a7a7a !important',
  },
  '.cm-lineNumbers .cm-gutterElement': {
    padding: '0 8px',
    fontSize: '0.8125rem',
    color: '#7a7a7a !important',
  },
  '.cm-activeLineGutter': {
    backgroundColor: '#242424 !important',
  },
  '.cm-activeLine': {
    backgroundColor: '#242424 !important',
  },
  '.cm-selectionBackground': {
    backgroundColor: '#3a3a3a !important',
  },
  '.cm-keyword': { color: '#8ab4f8 !important', fontWeight: 'bold' },
  '.cm-string': { color: '#34a853 !important' },
  '.cm-number': { color: '#fbbc04 !important' },
  '.cm-operator': { color: '#e8eaed !important' },
  '.cm-variable': { color: '#8ab4f8 !important' },
  '.cm-builtin': { color: '#8ab4f8 !important' },
  '.cm-comment': { color: '#9aa0a6 !important', fontStyle: 'italic' },
});
