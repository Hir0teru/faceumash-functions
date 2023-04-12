import * as functions from 'firebase-functions'
import * as admin from 'firebase-admin'
import {
  Firestore,
  DocumentSnapshot,
  QuerySnapshot,
  CollectionReference,
  AggregateQuerySnapshot,
  AggregateField,
  DocumentReference,
} from '@google-cloud/firestore'

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

export const aggregateRanking = functions.https.onCall(async () => {
  try {
    const charactersSnapshot = await db.collection('develop').doc('ZVP3ieLUu9RTLQN8vkIe').get()
    if (charactersSnapshot.exists) {
      const characters: Character[] = charactersSnapshot.data()?.characters
      const dictionary: Record<string, string> = createDictionaryFromCharacters(characters)
      const count: number = await getRatingsCount()
      const ranking: Ranking = await aggregateRatings(count, dictionary)
      const rankingRef: DocumentReference = db.collection('ranking').doc()
      await rankingRef.set({ ranking })
      return { success: true }
    } else {
      return { message: 'Document not found.' }
    }
  } catch (error: unknown) {
    throw new functions.https.HttpsError('unknown', '')
  }
})

export const createDictionaryFromCharacters = (characters: Character[]): Record<string, string> => {
  return characters.reduce((dictionary, character) => {
    dictionary[character.id] = character.name
    return dictionary
  }, {} as Record<string, string>)
}

export const getRatingsCount = async (): Promise<number> => {
  const ratingsRef: CollectionReference = db.collection('ratings')
  const snapshot: AggregateQuerySnapshot<{ count: AggregateField<number> }> = await ratingsRef
    .count()
    .get()
  return snapshot.data().count
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

  return result.sort((a, b) => a.rating - b.rating)
}
