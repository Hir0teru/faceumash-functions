import * as functions from 'firebase-functions'
import * as admin from 'firebase-admin'
import * as cors from 'cors'
import { config } from 'firebase-functions'
import {
  Firestore,
  DocumentSnapshot,
  QuerySnapshot,
  CollectionReference,
  DocumentReference,
  FieldValue,
} from '@google-cloud/firestore'

import { Request, Response } from 'express'

admin.initializeApp()

export type Character = {
  id: string
  name: string
  url: string
}

type Shard = {
  count: number
}

type Rating = {
  id: string
  name: string
  rating: number
}

type Ranking = Rating[]

const db: Firestore = admin.firestore()

export const aggregateRanking = functions.https.onRequest(async (req: Request, res: Response) => {
  const corsHandler = cors({ origin: true })

  corsHandler(req, res, async () => {
    if (req.method !== 'POST') {
      res.status(405).json({ message: 'Method not allowed.' })
      return
    }

    const apiKey = req.header('x-api-key')
    const expectedApiKey = config().api.key
    if (!apiKey || apiKey !== expectedApiKey) {
      res.status(403).json({ message: 'Forbidden.' })
      return
    }

    try {
      const charactersSnapshot = await db.collection('develop').doc('ZVP3ieLUu9RTLQN8vkIe').get()
      if (charactersSnapshot.exists) {
        const characters: Character[] = charactersSnapshot.data()?.characters
        const dictionary: Record<string, string> = createDictionaryFromCharacters(characters)
        const count = 89
        const ranking: Ranking = await aggregateRatings(count, dictionary)
        const rankingRef: DocumentReference = db.collection('ranking').doc()
        await rankingRef.set({ ranking, createdAt: FieldValue.serverTimestamp() })
        res.status(200).json({ success: true })
      } else {
        res.status(404).json({ message: 'Document not found.' })
      }
    } catch (error: unknown) {
      res.status(500).json({ message: 'An error occurred.' })
    }
  })
})

export const createDictionaryFromCharacters = (characters: Character[]): Record<string, string> => {
  return characters.reduce((dictionary, character) => {
    dictionary[character.id] = character.name
    return dictionary
  }, {} as Record<string, string>)
}

export const aggregateRatings = async (
  n: number,
  dictionary: Record<string, string>,
): Promise<Ranking> => {
  const result: Ranking = []

  for (let i = 0; i < n; i++) {
    const id: string = i.toString().padStart(4, '0')
    const shardsRef: CollectionReference = db.collection('ratings').doc(id).collection('shards')
    const shardsSnapshot: QuerySnapshot = await shardsRef.get()

    const rating: number = shardsSnapshot.docs.reduce(
      (accumulator: number, shardDoc: DocumentSnapshot) => {
        const shardData: Shard = shardDoc.data() as Shard
        return accumulator + shardData.count
      },
      0,
    )

    const name = dictionary[id] || 'Unknown'
    result.push({ id, name, rating })
  }

  return result.sort((a, b) => b.rating - a.rating)
}
