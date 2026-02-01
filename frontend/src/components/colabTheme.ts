import { EditorView } from '@codemirror/view';

// Dark theme with rim highlights for CodeMirror
const darkBackground = '#000000';
const darkGray = '#000000';
const mediumGray = '#000000';
const accentPink = '#ff096c';
const textPrimary = '#E6EDF3';
const textSecondary = '#9AA0A6';

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
    fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
  },
  '.cm-content': {
    padding: '8px 0',
    fontSize: '0.875rem',
    fontFamily: "'Consolas', 'Menlo', 'Monaco', 'Courier New', monospace",
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
    color: `${accentPink} !important`,
  },
  '.cm-activeLine': {
    backgroundColor: `${darkGray} !important`,
  },
  '.cm-selectionBackground': {
    backgroundColor: `${mediumGray} !important`,
  },
  '.cm-keyword': { color: `${accentPink} !important`, fontWeight: 'bold' },
  '.cm-string': { color: `${accentPink} !important` },
  '.cm-number': { color: `${accentPink} !important` },
  '.cm-operator': { color: `${textPrimary} !important` },
  '.cm-variable': { color: `${accentPink} !important` },
  '.cm-builtin': { color: `${accentPink} !important` },
  '.cm-comment': { color: `${textSecondary} !important`, fontStyle: 'italic' },
});
