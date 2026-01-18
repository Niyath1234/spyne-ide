#!/bin/bash

# Script to push local changes after merging remote changes
# This preserves all your local commits

set -e

echo "=== Preserving and Pushing Your Local Changes ==="
echo ""

# Show current status
echo "Your local commits (to be pushed):"
git log --oneline origin/main..HEAD 2>/dev/null || git log --oneline -6
echo ""

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo "⚠️  You have uncommitted changes:"
    git status --short
    echo ""
    read -p "Do you want to commit them? (y/n): " commit_changes
    if [ "$commit_changes" = "y" ]; then
        git add .
        read -p "Enter commit message: " commit_msg
        git commit -m "$commit_msg"
    fi
fi

# Try to pull remote changes
echo ""
echo "Attempting to pull remote changes..."
if git pull origin main --no-rebase 2>&1; then
    echo "✓ Successfully merged remote changes"
else
    echo "⚠️  Pull failed or no remote changes to merge"
fi

# Push your changes
echo ""
echo "Pushing your local commits..."
CURRENT_BRANCH=$(git branch --show-current)
if git push -u origin "$CURRENT_BRANCH" 2>&1; then
    echo ""
    echo "✓ Successfully pushed all your changes!"
    echo "Repository: https://github.com/Niyath1234/RCA-Engine"
else
    echo ""
    echo "❌ Push failed. You may need to:"
    echo "   1. Resolve merge conflicts if any"
    echo "   2. Or use: git push origin $CURRENT_BRANCH --force (if you're sure)"
fi

