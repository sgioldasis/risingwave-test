import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [
        react({
            babel: {
                plugins: [
                    "@babel/plugin-transform-optional-chaining",
                    "@babel/plugin-transform-nullish-coalescing-operator"
                ],
            },
        }),
    ],
    server: {
        host: true,
        port: 3000,
        proxy: {
            '/api': 'http://localhost:8000'
        }
    }
})
